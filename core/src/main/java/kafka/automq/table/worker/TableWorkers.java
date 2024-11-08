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

package kafka.automq.table.worker;

import kafka.automq.table.Channel;
import kafka.automq.table.events.CommitRequest;
import kafka.automq.table.events.CommitResponse;
import kafka.automq.table.events.Envelope;
import kafka.automq.table.events.Event;
import kafka.automq.table.events.EventType;
import kafka.automq.table.events.WorkerOffset;
import kafka.cluster.Partition;

import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.FetchIsolation;

import com.automq.stream.utils.Threads;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static kafka.automq.table.TableManager.ICEBERG_TABLE_TOPIC_PREFIX;

public class TableWorkers {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableWorkers.class);
    private static final ScheduledExecutorService SCHEDULER = Threads.newSingleThreadScheduledExecutor("table-workers", true, LOGGER);
    private final Catalog catalog;
    private final Channel channel;
    private final Channel.SubChannel subChannel;
    private final Map<String, Map<Integer, Partition>> topic2partitions = new ConcurrentHashMap<>();

    public TableWorkers(Catalog catalog, Channel channel) {
        this.catalog = catalog;
        this.channel = channel;
        this.subChannel = channel.subscribeControl();
        SCHEDULER.scheduleWithFixedDelay(this::run, 1, 1, TimeUnit.SECONDS);
    }

    public void link(Partition partition) {
        LOGGER.info("link partition {}", partition.topicPartition());
        topic2partitions.compute(partition.topicPartition().topic(), (topic, partitions) -> {
            if (partitions == null) {
                partitions = new ConcurrentHashMap<>();
            }
            partitions.put(partition.topicPartition().partition(), partition);
            return partitions;
        });
    }

    public void unlink(Partition partition) {
        // TODO
    }

    public void run() {
        try {
            run0();
        } catch (Exception e) {
            LOGGER.error("Error in table coordinator", e);
        }
    }

    public void run0() throws Exception {
        Envelope envelope = subChannel.poll();
        if (envelope == null) {
            return;
        }
        CommitRequest request = envelope.event().payload();
        LOGGER.info("receive commit request {}", request);
        String topic = request.topic();
        Map<Integer, Partition> partitions = topic2partitions.get(topic);
        if (partitions == null) {
            return;
        }
        TableIdentifier tableIdentifier = TableIdentifier.of(topic.substring(ICEBERG_TABLE_TOPIC_PREFIX.length()));
        Table table = catalog.loadTable(tableIdentifier);
        Schema schema = table.schema();
        TaskWriter<Record> writer = writer(table);
        List<WorkerOffset> nextOffsets = new ArrayList<>();
        for (WorkerOffset workerOffset : request.offsets()) {
            Partition partition = partitions.get(workerOffset.partition());
            if (partition == null) {
                continue;
            }
            // TODO: read to end
            FetchDataInfo fetchDataInfo = partition.log().get().readAsync(
                workerOffset.offset(),
                1024 * 1024,
                FetchIsolation.TXN_COMMITTED,
                true
            ).get();
            // TODO: transform abstraction
            long nextOffset = workerOffset.offset();
            Iterator<org.apache.kafka.common.record.Record> it = fetchDataInfo.records.records().iterator();
            while (it.hasNext()) {
                org.apache.kafka.common.record.Record record = it.next();
                GenericRecord parquetRecord = GenericRecord.create(schema);
                parquetRecord.setField("timestamp", record.timestamp());
                parquetRecord.setField("key", bytesbuf2string(record.key()));
                parquetRecord.setField("value", bytesbuf2string(record.value()));
                writer.write(parquetRecord);
                nextOffset = record.offset() + 1;
            }
            nextOffsets.add(new WorkerOffset(workerOffset.partition(), 0, nextOffset));
        }
        WriteResult writeResult = writer.complete();
        CommitResponse commitResponse = new CommitResponse(
            Types.StructType.of(),
            0,
            request.commitId(),
            topic,
            nextOffsets,
            Arrays.asList(writeResult.dataFiles()),
            Arrays.asList(writeResult.deleteFiles())
        );
        LOGGER.info("send commit response {}", commitResponse);
        channel.send(new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, commitResponse));
    }

    private TaskWriter<org.apache.iceberg.data.Record> writer(Table table) {
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null, null, null)
            .setAll(new HashMap<>(table.properties()));

        OutputFileFactory fileFactory =
            OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(FileFormat.PARQUET)
                .build();

        return new UnpartitionedWriter<Record>(
            table.spec(),
            FileFormat.PARQUET,
            appenderFactory,
            fileFactory,
            table.io(),
            1024 * 1024
        );
    }

    private static String bytesbuf2string(ByteBuffer buf) {
        if (buf == null) {
            return "";
        }
        byte[] bytes = new byte[buf.remaining()];
        buf.slice().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
