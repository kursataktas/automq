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
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.iceberg.types.Types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class Main {
    private static final byte[] MAGIC_BYTES = new byte[] {(byte) 0xC2, (byte) 0x01};

    public static void main(String... args) throws IOException {
        CommitResponse resp = new CommitResponse(Types.StructType.of(), 0, UUID.randomUUID(), "topic",
            List.of(new WorkerOffset(1, 2, 3)),
            List.of(EventTestUtil.createDataFile()), List.of(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile()));
//        byte[] bytes = AvroEncoderUtil.encode(resp, resp.getSchema());
//        CommitResponse decoded = AvroEncoderUtil.decode(bytes);
        byte[] bytes = encode(resp);
        CommitResponse decoded = decode(bytes);
        System.out.println(decoded);
    }

    private static <T extends IndexedRecord> byte[] encode(T data) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DataOutputStream dataOut = new DataOutputStream(out);

            // Write the magic bytes
            dataOut.write(MAGIC_BYTES);

            // Write avro schema
            dataOut.writeUTF(data.getSchema().toString());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
            writer.write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        }
    }

    private static <T extends IndexedRecord> T decode(byte[] data) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(data, 0, data.length)) {
            DataInputStream dataInput = new DataInputStream(in);

            // Read the magic bytes
            byte header0 = dataInput.readByte();
            byte header1 = dataInput.readByte();

            // Read avro schema
            Schema avroSchema = new Schema.Parser().parse(dataInput.readUTF());

            // Decode the datum with the parsed avro schema.
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            DatumReader<T> reader = new SpecificDatumReader<>(avroSchema);
            return reader.read(null, decoder);
        }
    }
}
