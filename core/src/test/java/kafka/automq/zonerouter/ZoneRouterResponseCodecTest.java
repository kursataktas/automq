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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
public class ZoneRouterResponseCodecTest {

    @Test
    public void testCodec() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> map = new HashMap<>();
        map.put(new TopicPartition("test", 1), new ProduceResponse.PartitionResponse(Errors.UNKNOWN_LEADER_EPOCH));
        ProduceResponseData data = new ProduceResponse(map).data();
        ProduceResponseData decoded = ZoneRouterResponseCodec.decode(ZoneRouterResponseCodec.encode(data));
        assertEquals(data, decoded);
    }

}
