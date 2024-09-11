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
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

public class ZoneRouterResponseCodec {
    public static final byte PRODUCE_RESPONSE_BLOCK_MAGIC = 0x01;

    public static ByteBuf encode(ProduceResponseData produceResponseData) {
        short version = 11;
        ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
        int size = produceResponseData.size(objectSerializationCache, version);
        ByteBuf buf = Unpooled.buffer(1 /* magic */ + 2 /* version */ + size);
        buf.writeByte(PRODUCE_RESPONSE_BLOCK_MAGIC);
        buf.writeShort(version);
        produceResponseData.write(new ByteBufferAccessor(buf.nioBuffer(buf.writerIndex(), size)), objectSerializationCache, version);
        buf.writerIndex(buf.writerIndex() + size);
        return buf;
    }

    public static ProduceResponseData decode(ByteBuf buf) {
        byte magic = buf.readByte();
        if (magic != PRODUCE_RESPONSE_BLOCK_MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        short version = buf.readShort();
        ProduceResponseData produceResponseData = new ProduceResponseData();
        produceResponseData.read(new ByteBufferAccessor(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())), version);
        return produceResponseData;
    }

}
