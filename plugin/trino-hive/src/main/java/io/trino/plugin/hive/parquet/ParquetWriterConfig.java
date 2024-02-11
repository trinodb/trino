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
package io.trino.plugin.hive.parquet;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.trino.parquet.writer.ParquetWriterOptions;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.apache.parquet.column.ParquetProperties;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "hive.parquet.optimized-writer.enabled",
        "parquet.experimental-optimized-writer.enabled",
        "parquet.optimized-writer.enabled",
})
public class ParquetWriterConfig
{
    public static final String PARQUET_WRITER_MAX_BLOCK_SIZE = "2GB";
    public static final String PARQUET_WRITER_MIN_PAGE_SIZE = "8kB";
    public static final String PARQUET_WRITER_MAX_PAGE_SIZE = "8MB";
    public static final int PARQUET_WRITER_MIN_PAGE_VALUE_COUNT = 1000;
    public static final int PARQUET_WRITER_MAX_PAGE_VALUE_COUNT = 200_000;

    private DataSize blockSize = DataSize.of(128, MEGABYTE);
    private DataSize pageSize = DataSize.ofBytes(ParquetProperties.DEFAULT_PAGE_SIZE);
    private int pageValueCount = ParquetWriterOptions.DEFAULT_MAX_PAGE_VALUE_COUNT;
    private int batchSize = ParquetWriterOptions.DEFAULT_BATCH_SIZE;
    private double validationPercentage = 5;

    @MaxDataSize(PARQUET_WRITER_MAX_BLOCK_SIZE)
    public DataSize getBlockSize()
    {
        return blockSize;
    }

    @Config("parquet.writer.block-size")
    @LegacyConfig("hive.parquet.writer.block-size")
    public ParquetWriterConfig setBlockSize(DataSize blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    @MinDataSize(PARQUET_WRITER_MIN_PAGE_SIZE)
    @MaxDataSize(PARQUET_WRITER_MAX_PAGE_SIZE)
    public DataSize getPageSize()
    {
        return pageSize;
    }

    @Config("parquet.writer.page-size")
    @LegacyConfig("hive.parquet.writer.page-size")
    public ParquetWriterConfig setPageSize(DataSize pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    @Min(PARQUET_WRITER_MIN_PAGE_VALUE_COUNT)
    @Max(PARQUET_WRITER_MAX_PAGE_VALUE_COUNT)
    public int getPageValueCount()
    {
        return pageValueCount;
    }

    @Config("parquet.writer.page-value-count")
    public ParquetWriterConfig setPageValueCount(int pageValueCount)
    {
        this.pageValueCount = pageValueCount;
        return this;
    }

    @Config("parquet.writer.batch-size")
    @ConfigDescription("Maximum number of rows passed to the writer in each batch")
    public ParquetWriterConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    @DecimalMin("0.0")
    @DecimalMax("100.0")
    public double getValidationPercentage()
    {
        return validationPercentage;
    }

    @Config("parquet.writer.validation-percentage")
    @LegacyConfig("parquet.optimized-writer.validation-percentage")
    @ConfigDescription("Percentage of parquet files to validate after write by re-reading the whole file")
    public ParquetWriterConfig setValidationPercentage(double validationPercentage)
    {
        this.validationPercentage = validationPercentage;
        return this;
    }
}
