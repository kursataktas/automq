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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EventTest {

    @Test
    public void testCommitRequestCodec() throws IOException {
        Event event = new Event(1, EventType.COMMIT_REQUEST,
            new CommitRequest(UUID.randomUUID(), "topic-xx", List.of(new WorkerOffset(1, 2, 3)))
        );
        Event rst = AvroCodec.decode(AvroCodec.encode(event));

        assertEquals(event.timestamp(), rst.timestamp());
        assertEquals(event.type(), rst.type());
        CommitRequest req1 = event.payload();
        CommitRequest req2 = rst.payload();
        assertEquals(req1.commitId(), req2.commitId());
        assertEquals(req1.topic(), req2.topic());
        assertEquals(req1.offsets().size(), req2.offsets().size());
        assertTrue(workOffsetEqual(req1.offsets().get(0), req2.offsets().get(0)));
    }

    @Test
    public void testCommitResponseCodec() throws IOException {
        Event event = new Event(2, EventType.COMMIT_RESPONSE,
            new CommitResponse(
                Types.StructType.of(),
                233,
                UUID.randomUUID(),
                "topic",
                List.of(new WorkerOffset(1, 2, 3)),
                List.of(EventTestUtil.createDataFile()), List.of(EventTestUtil.createDeleteFile(),
                EventTestUtil.createDeleteFile()))
        );
        Event rst = AvroCodec.decode(AvroCodec.encode(event));
        assertEquals(event.timestamp(), rst.timestamp());
        assertEquals(event.type(), rst.type());
        CommitResponse resp1 = event.payload();
        CommitResponse resp2 = rst.payload();
        assertEquals(resp1.code(), resp2.code());
        assertEquals(resp1.commitId(), resp2.commitId());
        assertEquals(resp1.topic(), resp2.topic());
        assertEquals(resp1.nextOffsets().size(), resp2.nextOffsets().size());
        assertTrue(workOffsetEqual(resp1.nextOffsets().get(0), resp2.nextOffsets().get(0)));

        assertEquals(1, resp2.dataFiles().size());
        assertEquals(resp1.dataFiles().get(0).path(), resp2.dataFiles().get(0).path());

        assertEquals(2, resp2.deleteFiles().size());
        assertEquals(resp1.deleteFiles().get(0).path(), resp2.deleteFiles().get(0).path());
        assertEquals(resp1.deleteFiles().get(1).path(), resp2.deleteFiles().get(1).path());
    }

    private boolean workOffsetEqual(WorkerOffset o1, WorkerOffset o2) {
        return o1.partition() == o2.partition() && o1.epoch() == o2.epoch() && o1.offset() == o2.offset();
    }

    @Test
    public void test() throws IOException {
        DataFile dataFile = EventTestUtil.createDataFile();
        Types.StructType partitionType = Types.StructType.of();
        Types.StructType dataFileStruct = DataFile.getType(partitionType);
        Map<Types.StructType, String> dataFileNames = new HashMap<>();
        dataFileNames.put(dataFileStruct, "org.apache.iceberg.GenericDataFile");
        dataFileNames.put(partitionType, "org.apache.iceberg.PartitionData");
        org.apache.avro.Schema schema = AvroSchemaUtil.convert(dataFileStruct, dataFileNames);
        DataFile rst = AvroEncoderUtil.decode(AvroEncoderUtil.encode(dataFile, schema));
        System.out.println(rst);
    }

}
