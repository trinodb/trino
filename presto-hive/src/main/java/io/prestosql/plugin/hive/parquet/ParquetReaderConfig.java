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
package io.prestosql.plugin.hive.parquet;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ParquetReaderConfig
{
    private boolean failOnCorruptedStatistics = true;
    private DataSize maxReadBlockSize = new DataSize(16, MEGABYTE);

    public boolean isFailOnCorruptedStatistics()
    {
        return failOnCorruptedStatistics;
    }

    @Config("hive.parquet.fail-on-corrupted-statistics")
    @ConfigDescription("Fail when scanning Parquet files with corrupted statistics")
    public ParquetReaderConfig setFailOnCorruptedStatistics(boolean failOnCorruptedStatistics)
    {
        this.failOnCorruptedStatistics = failOnCorruptedStatistics;
        return this;
    }

    @NotNull
    public DataSize getMaxReadBlockSize()
    {
        return maxReadBlockSize;
    }

    @Config("hive.parquet.max-read-block-size")
    public ParquetReaderConfig setMaxReadBlockSize(DataSize maxReadBlockSize)
    {
        this.maxReadBlockSize = maxReadBlockSize;
        return this;
    }
}
