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

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;

public class EventTestUtil {
    public static DataFile createDataFile() {
        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .withMetrics(new Metrics(1L, Map.of(1, 2L, 3, 4L), null, null, null))
            .build();
    }

    public static DeleteFile createDeleteFile() {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes(1)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    }
}