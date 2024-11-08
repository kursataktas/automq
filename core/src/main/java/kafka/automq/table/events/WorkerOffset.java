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

package kafka.automq.table.events;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class WorkerOffset implements Element {
    private int partition;
    private int epoch;
    private long offset;
    private final Schema avroSchema;

    public static final Schema AVRO_SCHEMA = SchemaBuilder.builder()
        .record(WorkerOffset.class.getName())
        .fields()
        .name("partition")
        .type().intType().noDefault()
        .name("epoch")
        .type().intType().noDefault()
        .name("offset")
        .type().longType().noDefault()
        .endRecord();

    public WorkerOffset(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public WorkerOffset(int partition, int epoch, long offset) {
        this.partition = partition;
        this.epoch = epoch;
        this.offset = offset;
        this.avroSchema = AVRO_SCHEMA;
    }

    public int partition() {
        return partition;
    }

    public int epoch() {
        return epoch;
    }

    public long offset() {
        return offset;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.partition = (int) v;
                return;
            case 1:
                this.epoch = (int) v;
                return;
            case 2:
                this.offset = (long) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return partition;
            case 1:
                return epoch;
            case 2:
                return offset;
            default:
                throw new UnsupportedOperationException("Unknown field ordinal: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public String toString() {
        return "WorkerOffset{" +
            "partition=" + partition +
            ", epoch=" + epoch +
            ", offset=" + offset +
            '}';
    }
}
