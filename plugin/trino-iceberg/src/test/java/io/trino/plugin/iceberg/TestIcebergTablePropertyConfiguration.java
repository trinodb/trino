/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.iceberg.IcebergFileWriterFactory.getParquetWriterBlockSize;
import static io.trino.plugin.iceberg.IcebergPageSink.getTargetMaxFileSize;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTablePropertyConfiguration
{
    @Test
    public void testTablePropertiesUsedWhenSet()
    {
        // Both table properties set: 20MB target file size, 5MB parquet row group size
        Map<String, String> storageProperties = ImmutableMap.of(
                WRITE_TARGET_FILE_SIZE_BYTES, "20971520",
                PARQUET_ROW_GROUP_SIZE_BYTES, "5242880");

        long defaultTargetFileSize = DataSize.of(1, GIGABYTE).toBytes();
        DataSize defaultParquetBlockSize = DataSize.of(128, MEGABYTE);
        long targetFileSize = getTargetMaxFileSize(storageProperties, defaultTargetFileSize);
        DataSize parquetBlockSize = getParquetWriterBlockSize(storageProperties, defaultParquetBlockSize);

        assertThat(targetFileSize).isEqualTo(20971520L);
        assertThat(parquetBlockSize).isEqualTo(DataSize.of(5, MEGABYTE));
    }

    @Test
    public void testFallsBackToDefaultsWhenTablePropertiesNotSet()
    {
        // Storage properties WITHOUT the table properties
        Map<String, String> storageProperties = ImmutableMap.of();
        long defaultTargetFileSize = DataSize.of(1, GIGABYTE).toBytes();
        DataSize defaultParquetBlockSize = DataSize.of(128, MEGABYTE);

        long targetFileSize = getTargetMaxFileSize(storageProperties, defaultTargetFileSize);
        DataSize parquetBlockSize = getParquetWriterBlockSize(storageProperties, defaultParquetBlockSize);

        // Defaults: 1GB for target file size (from connector config), 128MB for parquet block size (from connector config)
        assertThat(targetFileSize).isEqualTo(DataSize.of(1, GIGABYTE).toBytes());
        assertThat(parquetBlockSize).isEqualTo(DataSize.of(128, MEGABYTE));
    }

    @Test
    public void testInvalidTablePropertiesFallBackToDefaults()
    {
        // Invalid table property values
        Map<String, String> storageProperties = ImmutableMap.of(
                WRITE_TARGET_FILE_SIZE_BYTES, "not-a-number",
                PARQUET_ROW_GROUP_SIZE_BYTES, "invalid-value");
        long defaultTargetFileSize = DataSize.of(1, GIGABYTE).toBytes();
        DataSize defaultParquetBlockSize = DataSize.of(128, MEGABYTE);

        long targetFileSize = getTargetMaxFileSize(storageProperties, defaultTargetFileSize);
        DataSize parquetBlockSize = getParquetWriterBlockSize(storageProperties, defaultParquetBlockSize);

        // Defaults: 1GB for target file size (from connector config), 128MB for parquet block size (from connector config)
        assertThat(targetFileSize).isEqualTo(DataSize.of(1, GIGABYTE).toBytes());
        assertThat(parquetBlockSize).isEqualTo(DataSize.of(128, MEGABYTE));
    }
}
