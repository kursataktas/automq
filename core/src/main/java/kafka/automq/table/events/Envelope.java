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

public class Envelope {
    private int partition;
    private long offset;
    private Event event;

    public Envelope(int partition, long offset, Event event) {
        this.partition = partition;
        this.offset = offset;
        this.event = event;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Event event() {
        return event;
    }

}
