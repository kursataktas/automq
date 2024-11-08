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

package kafka.automq.table;

import kafka.automq.table.coordinator.TableCoordinator;
import kafka.automq.table.worker.TableWorkers;
import kafka.cluster.Partition;
import kafka.server.streamaspect.PartitionLifecycleListener;

import org.apache.kafka.common.TopicPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableManager implements PartitionLifecycleListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableManager.class);
    public static final String ICEBERG_TABLE_TOPIC_PREFIX = "iceberg.";
    private final Catalog catalog;
    private final Channel channel;
    private final Map<TopicPartition, TableCoordinator> coordinators = new ConcurrentHashMap<>();
    private final TableWorkers tableWorkers;

    public TableManager() {
        Configuration conf = new Configuration();
        Map<String, String> options = new HashMap<>();
        options.put("warehouse", "s3a://warehouse/");
        options.put("ref", "main");
        options.put("uri", "http://localhost:19120/api/v2");

        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "admin");
        conf.set("fs.s3a.secret.key", "password");
        conf.set("fs.s3a.endpoint.region", "us-east-1");
        conf.set("fs.s3a.path.style.access", "true");
        this.catalog = CatalogUtil.loadCatalog("org.apache.iceberg.nessie.NessieCatalog", "nessie", options, conf);
        this.channel = new Channel();
        this.tableWorkers = new TableWorkers(catalog, channel);
    }

    @Override
    public void onOpen(Partition partition) {
        try {
            String topic = partition.topicPartition().topic();
            int partitionId = partition.topicPartition().partition();
            if (!topic.startsWith(ICEBERG_TABLE_TOPIC_PREFIX)) {
                return;
            }
            if (partitionId == 0) {
                // start coordinator
                coordinators.put(partition.topicPartition(), new TableCoordinator(catalog, topic, channel));
            }
            // start worker
            tableWorkers.link(partition);
        } catch (Throwable e) {
            LOGGER.error("onOpen", e);
        }
    }

    @Override
    public void onClose(Partition partition) {
        // TODO: close
    }
}
