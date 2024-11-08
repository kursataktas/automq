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
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.util.List;
import java.util.UUID;

public class CommitRequest implements Payload {
    private UUID commitId;
    private String topic;
    private List<WorkerOffset> offsets;
    private final Schema avroSchema;

    private static final Schema AVRO_SCHEMA = SchemaBuilder.builder().record(CommitRequest.class.getName())
        .fields()
        .name("commitId").type(UUID_SCHEMA).noDefault()
        .name("topic").type().stringType().noDefault()
        .name("offsets").type().array().items(WorkerOffset.AVRO_SCHEMA).noDefault()
        .endRecord();

    // used by avro deserialize reflection
    public CommitRequest(Schema schema) {
        this.avroSchema = schema;
    }

    public CommitRequest(UUID commitId, String topic, List<WorkerOffset> offsets) {
        this.commitId = commitId;
        this.topic = topic;
        this.offsets = offsets;
        this.avroSchema = AVRO_SCHEMA;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.commitId = Element.toUuid((GenericData.Fixed) v);
                return;
            case 1:
                this.topic = ((Utf8) v).toString();
                return;
            case 2:
                //noinspection unchecked
                this.offsets = (List<WorkerOffset>) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return Element.toFixed(commitId);
            case 1:
                return topic;
            case 2:
                return offsets;
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    public UUID commitId() {
        return commitId;
    }

    public String topic() {
        return topic;
    }

    public List<WorkerOffset> offsets() {
        return offsets;
    }

    @Override
    public String toString() {
        return "CommitRequest{" +
            "commitId=" + commitId +
            ", topic='" + topic + '\'' +
            ", offsets=" + offsets +
            '}';
    }
}
