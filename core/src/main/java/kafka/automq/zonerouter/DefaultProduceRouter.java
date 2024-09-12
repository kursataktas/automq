/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.zonerouter;

import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.Threads;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import kafka.automq.zonerouter.ZoneRouterProduceRequest.Flag;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticKafkaApis;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterRequest;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class DefaultProduceRouter implements ProduceRouter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProduceRouter.class);
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("router", true, LOGGER);
    private final ElasticKafkaApis kafkaApis;
    private final MetadataCache metadataCache;
    private final KafkaConfig kafkaConfig;
    private final AsyncSender asyncSender;
    private final ObjectStorage objectStorage;

    private final AtomicLong nextObjectId = new AtomicLong();
    private volatile Node node;
    private int batchSizeThreshold = 4 * 1024 * 1024;
    private int batchIntervalMs = 100;
    private final Time time = Time.SYSTEM;
    private final RouterOut routerOut = new RouterOut();
    private final RouterIn routerIn = new RouterIn();

    public DefaultProduceRouter(ElasticKafkaApis kafkaApis, MetadataCache metadataCache, KafkaConfig kafkaConfig,
        ObjectStorage objectStorage) {
        this.kafkaApis = kafkaApis;
        this.metadataCache = metadataCache;
        this.kafkaConfig = kafkaConfig;
        this.asyncSender = new AsyncSender.BrokersAsyncSender(
            kafkaConfig,
            kafkaApis.metrics(),
            Time.SYSTEM,
            ZoneRouterPack.ZONE_ROUTER_CLIENT_ID,
            new LogContext()
        );
        this.objectStorage = objectStorage;
    }

    @Override
    public void handleProduceAppend(
        short apiVersion,
        String clientId,
        int timeout,
        short requiredAcks,
        boolean internalTopicsAllowed,
        String transactionId,
        Map<TopicPartition, MemoryRecords> entriesPerPartition,
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
        Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback
    ) {
        // 通过 clientid 判断走什么模式
        routerOut.handleProduceAppendProxy(timeout, apiVersion, requiredAcks, internalTopicsAllowed, transactionId, entriesPerPartition, responseCallback, recordValidationStatsCallback);
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
        return routerIn.handleZoneRouterRequest(metadata);
    }

    private Node currentNode() {
        if (node != null) {
            return node;
        }
        synchronized (this) {
            if (node == null) {
                node = metadataCache.getAliveBrokerNode(kafkaConfig.nodeId(), kafkaConfig.interBrokerListenerName()).get();
            }
            return node;
        }
    }

    class RouterOut {
        private final Map<Node, BlockingQueue<ProxyRequest>> pendingRequests = new ConcurrentHashMap<>();
        private CompletableFuture<Void> lastRouterCf = CompletableFuture.completedFuture(null);
        private final AtomicInteger batchSize = new AtomicInteger();
        private long lastUploadTimestamp = 0;

        public RouterOut() {
            scheduler.scheduleWithFixedDelay(this::proxy0, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
        }

        public void handleProduceAppendProxy(
            int timeout,
            short apiVersion,
            short requiredAcks,
            boolean internalTopicsAllowed,
            String transactionId,
            Map<TopicPartition, MemoryRecords> entriesPerPartition,
            Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
            Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback
        ) {
            short flag = new Flag().internalTopicsAllowed(internalTopicsAllowed).value();
            Map<Node, ProxyRequest> requests = split(apiVersion, timeout, flag, requiredAcks, transactionId, entriesPerPartition);
            Map<TopicPartition, ProduceResponse.PartitionResponse> rst = new ConcurrentHashMap<>();
            CompletableFuture.allOf(
                requests.values()
                    .stream()
                    .map(request ->
                        request.cf
                            .thenAccept(rst::putAll)
                            .exceptionally(ex -> {
                                LOGGER.error("[UNEXPECTED]", ex);
                                return null;
                            })
                    ).toArray(CompletableFuture[]::new)
            ).thenAccept(nil -> responseCallback.accept(rst));

            requests.forEach((node, request) -> pendingRequests.compute(node, (n, queue) -> {
                if (queue == null) {
                    queue = new LinkedBlockingQueue<>();
                }
                queue.add(request);
                return queue;
            }));
            // TODO: if the node is current node, then handle it directly
            int size = requests.values().stream().mapToInt(r -> r.size).sum();
            if (batchSize.addAndGet(size) >= batchSizeThreshold || time.milliseconds() - batchIntervalMs >= lastUploadTimestamp) {
                scheduler.submit(this::proxy0);
            }
        }

        private void proxy0() {
            if (batchSize.get() < batchSizeThreshold && time.milliseconds() - batchIntervalMs < lastUploadTimestamp) {
                return;
            }

            lastUploadTimestamp = time.milliseconds();
            Map<Node, List<ProxyRequest>> node2requests = new HashMap<>();
            pendingRequests.forEach((node, queue) -> {
                List<ProxyRequest> requests = new ArrayList<>();
                queue.drainTo(requests);
                if (!requests.isEmpty()) {
                    node2requests.put(node, requests);
                    batchSize.addAndGet(-requests.stream().mapToInt(r -> r.size).sum());
                }
            });
            if (node2requests.isEmpty()) {
                return;
            }

            long objectId = nextObjectId.incrementAndGet();
            LOGGER.info("try write s3 objectId={}", objectId);
            ZoneRouterPackWriter writer = new ZoneRouterPackWriter(kafkaConfig.nodeId(), objectId, objectStorage);
            Map<Node, Position> node2position = new HashMap<>();
            node2requests.forEach((node, requests) -> {
                Position position = writer
                    .addProduceRequests(
                        requests
                            .stream()
                            .map(r -> new ZoneRouterProduceRequest(r.apiVersion, r.flag, r.data))
                            .collect(Collectors.toList())
                    );
                requests.forEach(ProxyRequest::afterRouter);
                node2position.put(node, position);
            });

            CompletableFuture<Void> writeCf = writer.close();
            CompletableFuture<Void> prevLastRouterCf = lastRouterCf;
            lastRouterCf = writeCf
                // Orderly send the router request.
                .thenCompose(nil -> prevLastRouterCf)
                .thenAccept(nil -> {
                    LOGGER.info("write s3 objectId={}", objectId);
                    node2position.forEach((node, position) -> {
                        RouterRecord routerRecord = new RouterRecord(kafkaConfig.nodeId(), (short) 0, objectId, position.position(), position.size());

                        AutomqZoneRouterRequest.Builder builder = new AutomqZoneRouterRequest.Builder(
                            new AutomqZoneRouterRequestData().setMetadata(routerRecord.encode().array())
                        );
                        List<ProxyRequest> proxyRequests = node2requests.get(node);
                        asyncSender.sendRequest(node, builder).thenAccept(clientResponse -> {
                            LOGGER.info("receive router response={}", clientResponse);
                            if (!clientResponse.hasResponse()) {
                                LOGGER.error("has no response cause by other error");
                                // TODO: callback the error
                                return;
                            }
                            AutomqZoneRouterResponse zoneRouterResponse = (AutomqZoneRouterResponse) clientResponse.responseBody();
                            handleRouterResponse(zoneRouterResponse, proxyRequests);
                        }).exceptionally(ex -> {
                            // TODO: callback the error
                            LOGGER.error("[UNEXPECTED]", ex);
                            return null;
                        });
                    });
                })
                .exceptionally(ex -> {
                    LOGGER.error("[UNEXPECTED]", ex);
                    return null;
                });
        }

        private void handleRouterResponse(AutomqZoneRouterResponse zoneRouterResponse,
            List<ProxyRequest> proxyRequests) {
            List<AutomqZoneRouterResponseData.Response> responses = zoneRouterResponse.data().responses();
            for (int i = 0; i < proxyRequests.size(); i++) {
                ProxyRequest proxyRequest = proxyRequests.get(i);
                AutomqZoneRouterResponseData.Response response = responses.get(i);
                ProduceResponseData produceResponseData = ZoneRouterResponseCodec.decode(Unpooled.wrappedBuffer(response.data()));
                response.setData(null); // gc the data
                Map<TopicPartition, ProduceResponse.PartitionResponse> rst = new HashMap<>();
                produceResponseData.responses().forEach(topicData -> {
                    topicData.partitionResponses().forEach(partitionData -> {
                        ProduceResponse.PartitionResponse partitionResponse = new ProduceResponse.PartitionResponse(
                            Errors.forCode(partitionData.errorCode()),
                            partitionData.baseOffset(),
                            0, // last offset , the network layer don't need
                            partitionData.logAppendTimeMs(),
                            partitionData.logStartOffset(),
                            partitionData.recordErrors().stream().map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage())).collect(Collectors.toList()),
                            partitionData.errorMessage(),
                            partitionData.currentLeader()
                        );
                        rst.put(new TopicPartition(topicData.name(), partitionData.index()), partitionResponse);
                    });
                });
                proxyRequest.cf.complete(rst);
            }
        }

        private Map<Node, ProxyRequest> split(
            short apiVersion,
            int timeout,
            short requiredAcks,
            short flag,
            String transactionId,
            Map<TopicPartition, MemoryRecords> entriesPerPartition
        ) {
            AtomicInteger size = new AtomicInteger();
            Map<Node, List<Map.Entry<TopicPartition, MemoryRecords>>> node2Entries = new HashMap<>();
            entriesPerPartition.forEach((tp, records) -> {
                Option<Node> nodeOpt = metadataCache
                    .getPartitionLeaderEndpoint(tp.topic(), tp.partition(), kafkaConfig.interBrokerListenerName());
                Node node;
                if (nodeOpt.isEmpty()) {
                    node = currentNode();
                } else {
                    node = nodeOpt.get();
                }
                node2Entries.compute(node, (n, list) -> {
                    if (list == null) {
                        list = new ArrayList<>();
                    }
                    size.addAndGet(records.sizeInBytes());
                    list.add(Map.entry(tp, records));
                    return list;
                });
            });
            Map<Node, ProxyRequest> rst = new HashMap<>();
            node2Entries.forEach((node, entries) -> {
                ProduceRequestData data = new ProduceRequestData();
                data.setTransactionalId(transactionId);
                data.setAcks(requiredAcks);
                data.setTimeoutMs(timeout);

                Map<String, Map<Integer, MemoryRecords>> topicData = new HashMap<>();
                entries.forEach(e -> {
                    TopicPartition tp = e.getKey();
                    MemoryRecords records = e.getValue();
                    topicData.compute(tp.topic(), (topicName, map) -> {
                        if (map == null) {
                            map = new HashMap<>();
                        }
                        map.put(tp.partition(), records);
                        return map;
                    });
                });
                ProduceRequestData.TopicProduceDataCollection list = new ProduceRequestData.TopicProduceDataCollection();
                topicData.forEach((topicName, partitionData) -> {
                    list.add(
                        new ProduceRequestData.TopicProduceData()
                            .setName(topicName)
                            .setPartitionData(
                                partitionData.entrySet()
                                    .stream()
                                    .map(e -> new ProduceRequestData.PartitionProduceData().setIndex(e.getKey()).setRecords(e.getValue()))
                                    .collect(Collectors.toList())
                            )
                    );
                });
                data.setTopicData(list);
                rst.put(node, new ProxyRequest(apiVersion, flag, data, size.get()));
            });
            return rst;
        }
    }

    class RouterIn {

        public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
            // TODO: handle any unexpected exception
            RouterRecord routerRecord = RouterRecord.decode(Unpooled.wrappedBuffer(metadata));
            LOGGER.info("receive router request={}", routerRecord);
            // TODO: ensure the order
            CompletableFuture<List<ZoneRouterProduceRequest>> readCf = new ZoneRouterPackReader(routerRecord.nodeId(), routerRecord.bucketId(), routerRecord.objectId(), objectStorage)
                .readProduceRequests(new Position(routerRecord.position(), routerRecord.size()));
            CompletableFuture<AutomqZoneRouterResponse> appendCf = readCf
                .thenCompose(produces -> {
                    List<CompletableFuture<AutomqZoneRouterResponseData.Response>> cfList = new ArrayList<>();
                    produces.stream().map(this::append).forEach(cfList::add);
                    return CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0])).thenApply(nil -> {
                        AutomqZoneRouterResponseData response = new AutomqZoneRouterResponseData();
                        cfList.forEach(cf -> response.responses().add(cf.join()));
                        return new AutomqZoneRouterResponse(response);
                    });
                });
            appendCf.exceptionally(ex -> {
                LOGGER.error("[UNEXPECTED]", ex);
                return null;
            });
            return appendCf;
        }

        private CompletableFuture<AutomqZoneRouterResponseData.Response> append(
            ZoneRouterProduceRequest zoneRouterProduceRequest) {
            Flag flag = new Flag(zoneRouterProduceRequest.flag());
            ProduceRequestData data = zoneRouterProduceRequest.data();
            LOGGER.info("read zone router request from s3, data={}", data);
            short apiVersion = zoneRouterProduceRequest.apiVersion();

            Map<TopicPartition, MemoryRecords> realEntriesPerPartition = new HashMap<>();
            data.topicData().forEach(topicData ->
                topicData.partitionData().forEach(partitionData ->
                    realEntriesPerPartition.put(
                        new TopicPartition(topicData.name(), partitionData.index()),
                        (MemoryRecords) partitionData.records()
                    )));

            CompletableFuture<AutomqZoneRouterResponseData.Response> cf = new CompletableFuture<>();
            // TODO: parallel request for different partitions
            kafkaApis.handleProduceAppendJavaCompatible(
                10000L,
                data.acks(),
                flag.internalTopicsAllowed(),
                data.transactionalId(),
                realEntriesPerPartition,
                rst -> {
                    // TODO: 在 router zone in 塞 node endpoint 信息
                    @SuppressWarnings("deprecation")
                    ProduceResponse produceResponse = new ProduceResponse(rst, 0, Collections.emptyList());
                    AutomqZoneRouterResponseData.Response response = new AutomqZoneRouterResponseData.Response()
                        .setData(ZoneRouterResponseCodec.encode(produceResponse.data()).array());
                    cf.complete(response);
                    return null;
                },
                rst -> null,
                apiVersion
            );
            return cf;
        }
    }

    static class ProxyRequest {
        short apiVersion;
        short flag;
        ProduceRequestData data;
        int size;
        List<TopicPartition> topicPartitions;
        CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> cf;

        public ProxyRequest(short apiVersion, short flag, ProduceRequestData data, int size) {
            this.apiVersion = apiVersion;
            this.flag = flag;
            this.data = data;
            this.size = size;
            this.cf = new CompletableFuture<>();
        }

        public void afterRouter() {
            topicPartitions = new ArrayList<>();
            data.topicData().forEach(topicData -> topicData.partitionData().forEach(partitionData -> {
                topicPartitions.add(new TopicPartition(topicData.name(), partitionData.index()));
            }));
            data = null; // gc the data
        }
    }

}
