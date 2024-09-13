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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;

public interface ProduceRouter {

    void handleProduceAppend(
        short apiVersion,
        ClientIdMetadata clientId,
        int timeout,
        short requiredAcks,
        boolean internalTopicsAllowed,
        String transactionId,
        Map<TopicPartition, MemoryRecords> entriesPerPartition,
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
        Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback
    );

    CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata);

    Optional<Node> getLeaderNode(TopicPartition topicPartition, ClientIdMetadata clientId, String listenerName);

}