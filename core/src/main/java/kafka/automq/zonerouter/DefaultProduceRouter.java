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
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Threads;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
import org.apache.kafka.common.message.MetadataResponseData;
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
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProduceRouter implements ProduceRouter, MetadataPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProduceRouter.class);
    private static final String NOOP_RACK = "";
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("router", true, LOGGER);
    private final ElasticKafkaApis kafkaApis;
    private final MetadataCache metadataCache;
    private final KafkaConfig kafkaConfig;
    private final AsyncSender asyncSender;
    private final ObjectStorage objectStorage;

    private final AtomicLong nextObjectId = new AtomicLong();
    private volatile Node node;
    private final String currentRack;
    private final int currentNodeId;
    private final int batchSizeThreshold = Systems.getEnvInt("AUTOMQ_MULTI_WRITE_BATCH_SIZE", 4 * 1024 * 1024);
    private final int batchIntervalMs = Systems.getEnvInt("AUTOMQ_MULTI_WRITE_BATCH_INTERVAL", 100);
    private final Time time = Time.SYSTEM;
    private final ProxyNodeMapping mapping;
    private final RouterOut routerOut;
    private final RouterIn routerIn;

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
        if (kafkaConfig.rack().isEmpty()) {
            throw new IllegalArgumentException("The node rack should be set when enable cross available zone router");
        }
        this.currentRack = kafkaConfig.rack().get();
        this.currentNodeId = kafkaConfig.nodeId();
        this.mapping = new ProxyNodeMapping();
        this.routerOut = new RouterOut();
        this.routerIn = new RouterIn();
    }

    @Override
    public void handleProduceAppend(
        short apiVersion,
        ClientIdMetadata clientId,
        int timeout,
        short requiredAcks,
        boolean internalTopicsAllowed,
        String transactionId,
        Map<TopicPartition, MemoryRecords> entriesPerPartition,
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
        Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback
    ) {
        if (clientId.rack() != null) {
            routerOut.handleProduceAppendProxy(timeout, apiVersion, clientId, requiredAcks, internalTopicsAllowed, transactionId, entriesPerPartition, responseCallback, recordValidationStatsCallback);
        } else {
            kafkaApis.handleProduceAppendJavaCompatible(
                timeout,
                requiredAcks,
                internalTopicsAllowed,
                transactionId,
                entriesPerPartition,
                rst -> {
                    responseCallback.accept(rst);
                    return null;
                },
                rst -> {
                    recordValidationStatsCallback.accept(rst);
                    return null;
                },
                apiVersion
            );
        }
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
        return routerIn.handleZoneRouterRequest(metadata);
    }

    @Override
    public List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(String clientId,
        List<MetadataResponseData.MetadataResponseTopic> topics) {
        return mapping.handleMetadataResponse(clientId, topics);
    }

    @Override
    public Optional<Node> getLeaderNode(String topicName, int partitionId, ClientIdMetadata clientId,
        String listenerName) {
        return mapping.getLeaderNode(topicName, partitionId, clientId, listenerName);
    }

    private Node currentNode() {
        if (node != null) {
            return node;
        }
        synchronized (this) {
            if (node == null) {
                node = metadataCache.getNode(kafkaConfig.nodeId()).node(kafkaConfig.interBrokerListenerName().value()).get();
            }
            return node;
        }
    }

    @Override
    public String name() {
        return "CrossAzRouter";
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        mapping.onChange(delta, newImage);
    }

    class ProxyNodeMapping {
        private volatile Map<Integer, BrokerRegistration> main2proxy = new HashMap<>();
        private volatile Map<String, Map<Integer, BrokerRegistration>> main2proxyByRack = new HashMap<>();
        private final String interBrokerListenerName;

        public ProxyNodeMapping() {
            this.interBrokerListenerName = kafkaConfig.interBrokerListenerName().value();
        }

        public Node getRouteOutNode(String topicName, int partition, ClientIdMetadata clientId) {
            BrokerRegistration target = metadataCache.getPartitionLeaderNode(topicName, partition);
            Node currentNode = currentNode();
            if (target == null) {
                return currentNode;
            }
            String clientRack = clientId.rack();
            if (clientRack == null) {
                return target.node(interBrokerListenerName).orElse(Node.noNode());
            }

            if (Objects.equals(clientRack, currentRack) || !main2proxyByRack.containsKey(clientRack)) {
                if (Objects.equals(currentRack, target.rack().orElse(null))) {
                    if (target.id() == currentNode.id()) {
                        return currentNode;
                    } else {
                        // The producer should refresh metadata and send to another node in the same rack
                        return Node.noNode();
                    }
                } else {
                    return target.node(interBrokerListenerName).orElse(Node.noNode());
                }
            } else {
                return Node.noNode();
            }
        }

        public Optional<Node> getLeaderNode(String topicName, int partitionId, ClientIdMetadata clientId,
            String listenerName) {
            BrokerRegistration target = metadataCache.getPartitionLeaderNode(topicName, partitionId);
            if (target == null) {
                return Optional.empty();
            }
            String clientRack = clientId.rack();
            if (clientRack == null) {
                return target.node(listenerName);
            }

            Map<Integer, BrokerRegistration> clientRackMain2proxy = main2proxyByRack.get(clientRack);
            if (clientRackMain2proxy == null) {
                // Cluster doesn't cover the client rack
                return target.node(listenerName);
            }
            BrokerRegistration proxy = clientRackMain2proxy.get(target.id());
            if (proxy == null) {
                return target.node(listenerName);
            }
            return proxy.node(listenerName);
        }

        public List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(String clientId,
            List<MetadataResponseData.MetadataResponseTopic> topics) {
            ClientIdMetadata clientIdMetadata = ClientIdMetadata.of(clientId);
            String clientRack = clientIdMetadata.rack();
            if (clientRack == null) {
                return topics;
            }
            Map<Integer, BrokerRegistration> clientRackMain2proxy = main2proxyByRack.get(clientRack);
            if (clientRackMain2proxy == null) {
                // Cluster doesn't cover the client rack
                return topics;
            }
            topics.forEach(metadataResponseTopic -> {
                metadataResponseTopic.partitions().forEach(metadataResponsePartition -> {
                    int mainLeaderId = metadataResponsePartition.leaderId();
                    if (mainLeaderId != -1) {
                        BrokerRegistration proxy = clientRackMain2proxy.get(mainLeaderId);
                        if (proxy != null) {
                            int proxyLeaderId = proxy.id();
                            if (proxyLeaderId != mainLeaderId) {
                                metadataResponsePartition.setLeaderId(proxyLeaderId);
                                metadataResponsePartition.setIsrNodes(List.of(proxyLeaderId));
                                metadataResponsePartition.setReplicaNodes(List.of(proxyLeaderId));
                            }
                        }
                    }
                });
            });
            return topics;
        }

        public void onChange(MetadataDelta delta, MetadataImage image) {
            if (delta.clusterDelta() == null || delta.clusterDelta().changedBrokers().isEmpty()) {
                return;
            }
            // categorize the brokers by rack
            Map<String, List<BrokerRegistration>> rack2brokers = new HashMap<>();
            image.cluster().brokers().forEach((nodeId, node) -> {
                if (node.fenced()) {
                    return;
                }
                rack2brokers.compute(node.rack().orElse(NOOP_RACK), (rack, list) -> {
                    if (list == null) {
                        list = new ArrayList<>();
                    }
                    list.add(node);
                    return list;
                });
            });

            rack2brokers.forEach((rack, brokers) -> brokers.sort(Comparator.comparingInt(BrokerRegistration::id)));
            Map<String, Map<Integer, BrokerRegistration>> newMain2proxyByRack = new HashMap<>();

            rack2brokers.keySet().forEach(proxyRack -> {
                // TODO: replace with consistent hash allocation and consider the node weight
                Map<Integer, BrokerRegistration> newMain2proxy = new HashMap<>();
                List<BrokerRegistration> proxyRackBrokers = rack2brokers.get(proxyRack);
                rack2brokers.forEach((rack, brokers) -> {
                    if (Objects.equals(rack, proxyRack)) {
                        return;
                    }
                    for (int i = 0; i < brokers.size(); i++) {
                        BrokerRegistration node = brokers.get(i);
                        newMain2proxy.put(node.id(), proxyRackBrokers.get(i % proxyRackBrokers.size()));
                    }
                });
                newMain2proxyByRack.put(proxyRack, newMain2proxy);
            });

            this.main2proxy = newMain2proxyByRack.get(currentRack);
            this.main2proxyByRack = newMain2proxyByRack;
        }
    }

    class RouterOut {
        private final Map<Node, BlockingQueue<ProxyRequest>> pendingRequests = new ConcurrentHashMap<>();
        private CompletableFuture<Void> lastRouterCf = CompletableFuture.completedFuture(null);
        private final AtomicInteger batchSize = new AtomicInteger();
        private long lastUploadTimestamp = 0;
        private final boolean perfMode = Systems.getEnvBool("AUTOMQ_PERF_MODE", true); // force route 2 / 3 traffic
        private final Map<Long, Boolean> perfModeRouterMap = new ConcurrentHashMap<>();
        private final int routeBase = Systems.getEnvInt("AUTOMQ_PERF_MODE_ROUTE_BASE", 3);
        private final AtomicInteger perfRouterIndex = new AtomicInteger();

        public RouterOut() {
            scheduler.scheduleWithFixedDelay(this::proxy0, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
        }

        public void handleProduceAppendProxy(
            int timeout,
            short apiVersion,
            ClientIdMetadata clientId,
            short requiredAcks,
            boolean internalTopicsAllowed,
            String transactionId,
            Map<TopicPartition, MemoryRecords> entriesPerPartition,
            Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
            Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback
        ) {
            entriesPerPartition.entrySet().iterator().next().getValue().batches().iterator().next().producerId();
            short flag = new Flag().internalTopicsAllowed(internalTopicsAllowed).value();
            Map<Node, ProxyRequest> requests = split(apiVersion, clientId, timeout, flag, requiredAcks, transactionId, entriesPerPartition);
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

            boolean forceRoute = perfMode && isRouteInPerfMode(entriesPerPartition);
            requests.forEach((node, request) -> {
                if (node.id() == Node.noNode().id()) {
                    request.completeWithNotLeaderNotFollower();
                    return;
                }
                if (node.id() == currentNodeId && !forceRoute) {
                    // TODO: submit to another thread
                    kafkaApis.handleProduceAppendJavaCompatible(
                        timeout, requiredAcks, internalTopicsAllowed, transactionId,
                        produceRequestToMap(request.data),
                        responseCallbackRst -> {
                            request.cf.complete(responseCallbackRst);
                            return null;
                        },
                        recordValidationStatsCallbackRst -> {
                            recordValidationStatsCallback.accept(recordValidationStatsCallbackRst);
                            return null;
                        },
                        apiVersion
                    );
                } else {
                    pendingRequests.compute(node, (n, queue) -> {
                        if (queue == null) {
                            queue = new LinkedBlockingQueue<>();
                        }
                        queue.add(request);
                        batchSize.addAndGet(request.size);
                        return queue;
                    });
                }
            });
            if (batchSize.get() >= batchSizeThreshold || time.milliseconds() - batchIntervalMs >= lastUploadTimestamp) {
                scheduler.submit(this::proxy0);
            }
        }

        private boolean isRouteInPerfMode(Map<TopicPartition, MemoryRecords> entriesPerPartition) {
            long producerId = entriesPerPartition.entrySet().iterator().next().getValue().batches().iterator().next().producerId();
            return perfModeRouterMap.computeIfAbsent(producerId, k -> {
                int index = perfRouterIndex.incrementAndGet();
                return index % routeBase != 0;
            });
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
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("write s3 objectId={}", objectId);
                    }
                    // TODO: in / out metrics and type (要经过限流器 + 另外一个 type 类型统计)
                    List<CompletableFuture<Void>> sendCfList = new ArrayList<>();
                    node2position.forEach((node, position) -> {
                        RouterRecord routerRecord = new RouterRecord(kafkaConfig.nodeId(), (short) 0, objectId, position.position(), position.size());

                        AutomqZoneRouterRequest.Builder builder = new AutomqZoneRouterRequest.Builder(
                            new AutomqZoneRouterRequestData().setMetadata(routerRecord.encode().array())
                        );
                        List<ProxyRequest> proxyRequests = node2requests.get(node);
                        CompletableFuture<Void> sendCf = asyncSender.sendRequest(node, builder).thenAccept(clientResponse -> {
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("receive router response={}", clientResponse);
                            }
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
                        sendCfList.add(sendCf);
                    });
                    CompletableFuture.allOf(sendCfList.toArray(new CompletableFuture[0])).whenComplete((nil2, ex) -> {
                        ObjectStorage.ObjectPath path = writer.objectPath();
                        objectStorage.delete(List.of(path)).exceptionally(ex2 -> {
                            LOGGER.error("delete {} fail", path, ex);
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

        /**
         * Split the produce request to different nodes
         */
        private Map<Node, ProxyRequest> split(
            short apiVersion,
            ClientIdMetadata clientId,
            int timeout,
            short requiredAcks,
            short flag,
            String transactionId,
            Map<TopicPartition, MemoryRecords> entriesPerPartition
        ) {
            Map<Node, List<Map.Entry<TopicPartition, MemoryRecords>>> node2Entries = new HashMap<>();
            entriesPerPartition.forEach((tp, records) -> {
                Node node = mapping.getRouteOutNode(tp.topic(), tp.partition(), clientId);
                node2Entries.compute(node, (n, list) -> {
                    if (list == null) {
                        list = new ArrayList<>();
                    }
                    list.add(Map.entry(tp, records));
                    return list;
                });
            });
            Map<Node, ProxyRequest> rst = new HashMap<>();
            node2Entries.forEach((node, entries) -> {
                AtomicInteger size = new AtomicInteger();
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
                        size.addAndGet(records.sizeInBytes());
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
        private CompletableFuture<Void> lastRouterCf = CompletableFuture.completedFuture(null);
        private final ExecutorService executor = Threads.newFixedFastThreadLocalThreadPoolWithMonitor(1, "router-in", true, LOGGER);

        public synchronized CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
            // TODO: handle any unexpected exception
            RouterRecord routerRecord = RouterRecord.decode(Unpooled.wrappedBuffer(metadata));
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("receive router request={}", routerRecord);
            }
            CompletableFuture<List<ZoneRouterProduceRequest>> readCf = new ZoneRouterPackReader(routerRecord.nodeId(), routerRecord.bucketId(), routerRecord.objectId(), objectStorage)
                .readProduceRequests(new Position(routerRecord.position(), routerRecord.size()));
            // Orderly handle the request
            CompletableFuture<Void> prevLastRouterCf = lastRouterCf;
            CompletableFuture<AutomqZoneRouterResponse> appendCf = readCf
                .thenCompose(rst -> prevLastRouterCf.thenApply(nil -> rst))
                .thenComposeAsync(produces -> {
                    List<CompletableFuture<AutomqZoneRouterResponseData.Response>> cfList = new ArrayList<>();
                    produces.stream().map(this::append).forEach(cfList::add);
                    return CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0])).thenApply(nil -> {
                        AutomqZoneRouterResponseData response = new AutomqZoneRouterResponseData();
                        cfList.forEach(cf -> response.responses().add(cf.join()));
                        return new AutomqZoneRouterResponse(response);
                    });
                }, executor);
            this.lastRouterCf = appendCf
                .thenAccept(rst -> {
                }).exceptionally(ex -> {
                    LOGGER.error("[UNEXPECTED]", ex);
                    return null;
                });
            return appendCf;
        }

        private CompletableFuture<AutomqZoneRouterResponseData.Response> append(
            ZoneRouterProduceRequest zoneRouterProduceRequest) {
            Flag flag = new Flag(zoneRouterProduceRequest.flag());
            ProduceRequestData data = zoneRouterProduceRequest.data();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("read zone router request from s3, data={}", data);
            }

            Map<TopicPartition, MemoryRecords> realEntriesPerPartition = produceRequestToMap(data);
            short apiVersion = zoneRouterProduceRequest.apiVersion();
            CompletableFuture<AutomqZoneRouterResponseData.Response> cf = new CompletableFuture<>();
            // TODO: parallel request for different partitions
            kafkaApis.handleProduceAppendJavaCompatible(
                10000L,
                data.acks(),
                flag.internalTopicsAllowed(),
                data.transactionalId(),
                realEntriesPerPartition,
                rst -> {
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

    static Map<TopicPartition, MemoryRecords> produceRequestToMap(ProduceRequestData data) {
        Map<TopicPartition, MemoryRecords> realEntriesPerPartition = new HashMap<>();
        data.topicData().forEach(topicData ->
            topicData.partitionData().forEach(partitionData ->
                realEntriesPerPartition.put(
                    new TopicPartition(topicData.name(), partitionData.index()),
                    (MemoryRecords) partitionData.records()
                )));
        return realEntriesPerPartition;
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
            this.topicPartitions = new ArrayList<>();
            this.data.topicData().forEach(topicData -> topicData.partitionData().forEach(partitionData -> {
                topicPartitions.add(new TopicPartition(topicData.name(), partitionData.index()));
            }));
        }

        public void afterRouter() {
            data = null; // gc the data
        }

        public void completeWithNotLeaderNotFollower() {
            Map<TopicPartition, ProduceResponse.PartitionResponse> rst = new HashMap<>();
            topicPartitions.forEach(tp -> rst.put(tp, new ProduceResponse.PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER, -1, -1, -1, -1, Collections.emptyList(), "")));
            cf.complete(rst);
        }
    }

}
