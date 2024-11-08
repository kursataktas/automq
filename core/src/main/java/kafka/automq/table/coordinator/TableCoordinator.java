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

package kafka.automq.table.coordinator;

import kafka.automq.table.Channel;
import kafka.automq.table.events.CommitRequest;
import kafka.automq.table.events.CommitResponse;
import kafka.automq.table.events.Envelope;
import kafka.automq.table.events.Event;
import kafka.automq.table.events.EventType;
import kafka.automq.table.events.WorkerOffset;

import com.automq.stream.utils.Threads;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static kafka.automq.table.TableManager.ICEBERG_TABLE_TOPIC_PREFIX;

public class TableCoordinator implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableCoordinator.class);
    private static final ScheduledExecutorService SCHEDULER = Threads.newSingleThreadScheduledExecutor("table-coordinator", true, LOGGER);
    private final Catalog catalog;
    private final String topic;
    private final Channel channel;
    private final ScheduledFuture<?> future;
    private final Channel.SubChannel subChannel;
    private Table table;

    public TableCoordinator(Catalog catalog, String topic, Channel channel) {
        this.catalog = catalog;
        this.topic = topic;
        this.channel = channel;
        // TODO: kraft metadata to get current partition count and leader epoch
        // TODO: metastream to persist meta
        // TODO: set commit interval based on config
        this.future = SCHEDULER.scheduleWithFixedDelay(this::run, 1, 1, TimeUnit.SECONDS);
        // TODO: recover worker offset from metastream and iceberg last snapshot
        // TODO: load from metastream
        this.subChannel = channel.subscribeData(topic, 0);
        // TODO: auto create table when write
        TableIdentifier tableIdentifier = TableIdentifier.of(topic.substring(ICEBERG_TABLE_TOPIC_PREFIX.length()));
        Schema schema = new Schema(
            Types.NestedField.optional(1, "timestamp", Types.LongType.get()),
            Types.NestedField.optional(2, "key", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.StringType.get())
        );
        try {
            this.table = catalog.loadTable(tableIdentifier);
        } catch (NoSuchTableException e) {
            this.table = catalog.createTable(tableIdentifier, schema);
        }
    }

    private long[] nextOffsets = new long[1];
    private CommitRequest request;
    private BitSet readyPartitions;
    private int awaitReadyPartitionCount;
    private List<DataFile> dataFiles;
    private List<DeleteFile> deleteFiles;
    private CommitStatus status = CommitStatus.COMMIT;

    public void run() {
        try {
            run0();
        } catch (Exception e) {
            LOGGER.error("Error in table coordinator", e);
        }
    }

    public void run0() throws Exception {
        // run in single thread
        if (status == CommitStatus.COMMIT) {
            List<WorkerOffset> offsets = new ArrayList<>(nextOffsets.length);
            for (int i = 0; i < nextOffsets.length; i++) {
                // TODO: epoch
                offsets.add(new WorkerOffset(i, 0, nextOffsets[i]));
            }
            readyPartitions = new BitSet(1);
            awaitReadyPartitionCount = 1;
            dataFiles = new ArrayList<>();
            deleteFiles = new ArrayList<>();
            request = new CommitRequest(UUID.randomUUID(), topic, offsets);
            channel.send(new Event(System.currentTimeMillis(), EventType.COMMIT_REQUEST, request));
            status = CommitStatus.REQUEST;
            LOGGER.info("Send Commit Request {}", request);
        } else {
            Envelope envelope;
            for (; ; ) {
                envelope = subChannel.poll();
                if (envelope == null) {
                    return;
                }
                CommitResponse commitResponse = envelope.event().payload();
                if (!request.commitId().equals(commitResponse.commitId())) {
                    continue;
                }
                LOGGER.info("Receive: {}", commitResponse);
                dataFiles.addAll(commitResponse.dataFiles());
                deleteFiles.addAll(commitResponse.deleteFiles());
                for (WorkerOffset nextOffset : commitResponse.nextOffsets()) {
                    nextOffsets[nextOffset.partition()] = nextOffset.offset();
                    if (!readyPartitions.get(nextOffset.partition())) {
                        readyPartitions.set(nextOffset.partition());
                        awaitReadyPartitionCount--;
                    }
                }
                if (awaitReadyPartitionCount == 0) {
                    status = CommitStatus.DATA_READY;
                    if (!dataFiles.isEmpty() || !deleteFiles.isEmpty()) {
                        Transaction transaction = table.newTransaction();
                        if (!dataFiles.isEmpty()) {
                            AppendFiles appendFiles = transaction.newAppend();
                            dataFiles.forEach(appendFiles::appendFile);
                            appendFiles.commit();
                        }
                        if (!deleteFiles.isEmpty()) {
                            RowDelta rowDelta = transaction.newRowDelta();
                            deleteFiles.forEach(rowDelta::addDeletes);
                            rowDelta.commit();
                        }
                        transaction.commitTransaction();
                    } else {
                        LOGGER.info("Nothing need to be commited");
                    }
                    LOGGER.info("Commit complete {}", request);
                    status = CommitStatus.COMMIT;
                }
            }
        }
    }

    @Override
    public void close() {
        future.cancel(false);
    }

    enum CommitStatus {
        REQUEST,
        DATA_READY,
        COMMIT
    }
}
