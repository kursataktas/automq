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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RouterRecord {
    private static final short MAGIC = 0x01;
    private final int nodeId;
    private final short bucketId;
    private final long objectId;
    private final int position;
    private final int size;

    public RouterRecord(int nodeId, short bucketId, long objectId, int position, int size) {
        this.nodeId = nodeId;
        this.bucketId = bucketId;
        this.objectId = objectId;
        this.position = position;
        this.size = size;
    }

    public int nodeId() {
        return nodeId;
    }

    public short bucketId() {
        return bucketId;
    }

    public long objectId() {
        return objectId;
    }

    public int position() {
        return position;
    }

    public int size() {
        return size;
    }

    public ByteBuf encode() {
        ByteBuf buf = Unpooled.buffer(1 /* magic */ + 4 /* nodeId */ + 2 /* bucketId */ + 8 /* objectId */ + 4 /* position */ + 4 /* size */);
        buf.writeByte(MAGIC);
        buf.writeInt(nodeId);
        buf.writeShort(bucketId);
        buf.writeLong(objectId);
        buf.writeInt(position);
        buf.writeInt(size);
        return buf;
    }

    public static RouterRecord decode(ByteBuf buf) {
        buf = buf.slice();
        short magic = buf.readShort();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        int nodeId = buf.readInt();
        short bucketId = buf.readShort();
        long objectId = buf.readLong();
        int position = buf.readInt();
        int size = buf.readInt();
        return new RouterRecord(nodeId, bucketId, objectId, position, size);
    }

}
