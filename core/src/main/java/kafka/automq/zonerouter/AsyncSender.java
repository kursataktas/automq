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

import com.automq.stream.utils.Threads;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface AsyncSender {

    <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(
        Node node,
        AbstractRequest.Builder<T> requestBuilder
    );

    void initiateClose();

    void close();

    class BrokersAsyncSender implements AsyncSender {
        private static final Logger LOGGER = LoggerFactory.getLogger(BrokersAsyncSender.class);
        private final NetworkClient networkClient;
        private final Time time;
        private final ExecutorService executorService;
        private final AtomicBoolean shouldRun = new AtomicBoolean(true);

        public BrokersAsyncSender(
            KafkaConfig brokerConfig,
            Metrics metrics,
            Time time,
            String clientId,
            LogContext logContext
        ) {

            ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
                brokerConfig.interBrokerSecurityProtocol(),
                JaasContext.Type.SERVER,
                brokerConfig,
                brokerConfig.interBrokerListenerName(),
                brokerConfig.saslMechanismInterBrokerProtocol(),
                time,
                brokerConfig.saslInterBrokerHandshakeRequestEnable(),
                logContext
            );
            Selector selector = new Selector(
                NetworkReceive.UNLIMITED,
                brokerConfig.connectionsMaxIdleMs(),
                metrics,
                time,
                "zone-router",
                Collections.emptyMap(),
                false,
                channelBuilder,
                logContext
            );
            this.networkClient = new NetworkClient(
                selector,
                new ManualMetadataUpdater(),
                clientId,
                5,
                0,
                0,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                brokerConfig.replicaSocketReceiveBufferBytes(),
                brokerConfig.requestTimeoutMs(),
                brokerConfig.connectionSetupTimeoutMs(),
                brokerConfig.connectionSetupTimeoutMaxMs(),
                time,
                false,
                new ApiVersions(),
                logContext
            );
            this.time = time;
            executorService = Threads.newFixedThreadPoolWithMonitor(1, "zone-router", true, LOGGER);
            executorService.submit(this::run);
        }

        private final ConcurrentMap<Node, Queue<Request>> waitingSendRequests = new ConcurrentHashMap<>();

        @Override
        public <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(Node node,
            AbstractRequest.Builder<T> requestBuilder) {
            CompletableFuture<ClientResponse> cf = new CompletableFuture<>();
            waitingSendRequests.compute(node, (n, queue) -> {
                if (queue == null) {
                    queue = new LinkedList<>();
                }
                queue.add(new Request(requestBuilder, cf));
                return queue;
            });
            return cf;
        }

        private void run() {
            while (shouldRun.get()) {
                // TODO: graceful shutdown
                try {
                    long now = time.milliseconds();
                    waitingSendRequests.forEach((node, queue) -> {
                        if (queue.isEmpty()) {
                            return;
                        }
                        if (NetworkClientUtils.isReady(networkClient, node, now) || networkClient.ready(node, now)) {
                            Request request = queue.poll();
                            ClientRequest clientRequest = networkClient.newClientRequest(Integer.toString(node.id()), request.requestBuilder, now, true, 3000, new RequestCompletionHandler() {
                                @Override
                                public void onComplete(ClientResponse response) {
                                    request.cf.complete(response);
                                }
                            });
                            networkClient.send(clientRequest, now);
                        }
                    });
                    networkClient.poll(1, now);
                } catch (Throwable e) {
                    LOGGER.error("Processor get uncaught exception", e);
                }
            }
        }

        @Override
        public void initiateClose() {
            networkClient.initiateClose();
        }

        @Override
        public void close() {
            networkClient.close();
            shouldRun.set(false);
        }
    }

    class Request {
        final AbstractRequest.Builder<?> requestBuilder;
        final CompletableFuture<ClientResponse> cf;

        public Request(AbstractRequest.Builder<?> requestBuilder, CompletableFuture<ClientResponse> cf) {
            this.requestBuilder = requestBuilder;
            this.cf = cf;
        }
    }

}
