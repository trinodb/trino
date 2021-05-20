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
package io.trino.plugin.hive.benchmark;

/**
 * File formats from StandardFileFormats class captured in an enum for JMH benchmarks
 */
public enum BenchmarkFileFormat
{
    TRINO_RCBINARY(StandardFileFormats.TRINO_RCBINARY),
    TRINO_RCTEXT(StandardFileFormats.TRINO_RCTEXT),
    TRINO_ORC(StandardFileFormats.TRINO_ORC),
    TRINO_PARQUET(StandardFileFormats.TRINO_PARQUET),
    HIVE_RCBINARY(StandardFileFormats.HIVE_RCBINARY),
    HIVE_RCTEXT(StandardFileFormats.HIVE_RCTEXT),
    HIVE_ORC(StandardFileFormats.HIVE_ORC),
    HIVE_PARQUET(StandardFileFormats.HIVE_PARQUET),
    /**/;

    private final FileFormat format;

    BenchmarkFileFormat(FileFormat format)
    {
        this.format = format;
    }

    public FileFormat getFormat()
    {
        return format;
    }
}
