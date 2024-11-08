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

import org.apache.kafka.common.utils.ByteBufferInputStream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.GenericAvroReader;
import org.apache.iceberg.avro.GenericAvroWriter;
import org.apache.iceberg.types.Types;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CommitResponse implements Payload {
    private static final byte ICEBERG_MAGIC = 0x01;

    private int code;
    private UUID commitId;
    private String topic;
    private List<WorkerOffset> nextOffsets;
    private List<DataFile> dataFiles;
    private List<DeleteFile> deleteFiles;
    private final Schema avroSchema;
    private Schema dataFileSchema;
    private Schema deleteFileSchema;

    // used by avro deserialize reflection
    public CommitResponse(Schema schema) {
        this.avroSchema = schema;
    }

    public CommitResponse(Types.StructType partitionType, int code, UUID commitId, String topic,
        List<WorkerOffset> nextOffsets, List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
        this.code = code;
        this.commitId = commitId;
        this.topic = topic;
        this.nextOffsets = nextOffsets;
        this.dataFiles = dataFiles;
        this.deleteFiles = deleteFiles;

        Types.StructType dataFileStruct = DataFile.getType(partitionType);

        Map<Types.StructType, String> dataFileNames = new HashMap<>();
        dataFileNames.put(dataFileStruct, "org.apache.iceberg.GenericDataFile");
        dataFileNames.put(partitionType, "org.apache.iceberg.PartitionData");
        this.dataFileSchema = AvroSchemaUtil.convert(dataFileStruct, dataFileNames);

        Map<Types.StructType, String> deleteFileNames = new HashMap<>();
        deleteFileNames.put(dataFileStruct, "org.apache.iceberg.GenericDeleteFile");
        deleteFileNames.put(partitionType, "org.apache.iceberg.PartitionData");
        this.deleteFileSchema = AvroSchemaUtil.convert(dataFileStruct, deleteFileNames);

        this.avroSchema = SchemaBuilder.builder().record(CommitResponse.class.getName())
            .fields()
            .name("code").type().intType().noDefault()
            .name("commitId").type(UUID_SCHEMA).noDefault()
            .name("topic").type().stringType().noDefault()
            .name("nextOffsets").type().array().items(WorkerOffset.AVRO_SCHEMA).noDefault()
            .name("dataFiles").type().bytesType().noDefault()
            .name("deleteFiles").type().bytesType().noDefault()
            .endRecord();
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.code = (int) v;
                break;
            case 1:
                this.commitId = Element.toUuid((GenericData.Fixed) v);
                break;
            case 2:
                this.topic = ((Utf8) v).toString();
                break;
            case 3:
                //noinspection unchecked
                this.nextOffsets = (List<WorkerOffset>) v;
                break;
            case 4:
                //noinspection unchecked
                try {
                    this.dataFiles = decodeIcebergArray((ByteBuffer) v);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            case 5:
                //noinspection unchecked
                try {
                    this.deleteFiles = decodeIcebergArray((ByteBuffer) v);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return code;
            case 1:
                return Element.toFixed(commitId);
            case 2:
                return topic;
            case 3:
                return nextOffsets;
            case 4:
                try {
                    return ByteBuffer.wrap(encodeIcebergArray(dataFiles, dataFileSchema));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            case 5:
                try {
                    return ByteBuffer.wrap(encodeIcebergArray(deleteFiles, deleteFileSchema));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    public int code() {
        return code;
    }

    public UUID commitId() {
        return commitId;
    }

    public String topic() {
        return topic;
    }

    public List<WorkerOffset> nextOffsets() {
        return nextOffsets;
    }

    public List<DataFile> dataFiles() {
        return dataFiles;
    }

    public List<DeleteFile> deleteFiles() {
        return deleteFiles;
    }

    @Override
    public String toString() {
        return "CommitResponse{" +
            "code=" + code +
            ", commitId=" + commitId +
            ", topic='" + topic + '\'' +
            ", nextOffsets=" + nextOffsets +
            ", dataFiles=" + dataFiles +
            ", deleteFiles=" + deleteFiles +
            '}';
    }

    <T> byte[] encodeIcebergArray(List<T> list, Schema schema) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DataOutputStream dataOut = new DataOutputStream(out);

            // Write the magic bytes
            dataOut.write(ICEBERG_MAGIC);

            // Write avro schema
            dataOut.writeUTF(schema.toString());

            // Encode the datum with avro schema.
            DatumWriter<T> writer = GenericAvroWriter.create(schema);
            for (T datum : list) {
                ByteArrayOutputStream elementOut = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(elementOut, null);
                writer.write(datum, encoder);
                encoder.flush();
                byte[] elementOutBytes = elementOut.toByteArray();
                dataOut.writeInt(elementOutBytes.length);
                dataOut.write(elementOutBytes);
            }

            return out.toByteArray();
        }
    }

    <T> List<T> decodeIcebergArray(ByteBuffer data) throws IOException {
        try (
            ByteBufferInputStream in = new ByteBufferInputStream(data);
            DataInputStream dataInput = new DataInputStream(in)
        ) {
            // Read the magic bytes
            byte magic = dataInput.readByte();
            if (magic != ICEBERG_MAGIC) {
                throw new IllegalArgumentException(String.format("Unrecognized magic byte: 0x%02X", magic));
            }

            // Read avro schema
            Schema avroSchema = new Schema.Parser().parse(dataInput.readUTF());

            List<T> list = new ArrayList<>();
            // Decode the datum with the parsed avro schema.
            DatumReader<T> reader = GenericAvroReader.create(avroSchema);
            reader.setSchema(avroSchema);
            while (in.available() != 0) {
                int length = dataInput.readInt();
                BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(BoundedInputStream.builder().setInputStream(in).setMaxCount(length).get(), null);
                list.add(reader.read(null, binaryDecoder));
            }
            return list;
        }
    }
}
