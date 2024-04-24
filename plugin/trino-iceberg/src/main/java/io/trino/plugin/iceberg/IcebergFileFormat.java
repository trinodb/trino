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

import org.apache.iceberg.FileFormat;

public enum IcebergFileFormat
{
    ORC,
    PARQUET,
    AVRO,
    /**/;

    public FileFormat toIceberg()
    {
        return switch (this) {
            case ORC -> FileFormat.ORC;
            case PARQUET -> FileFormat.PARQUET;
            case AVRO -> FileFormat.AVRO;
        };
    }

    public static IcebergFileFormat fromIceberg(FileFormat format)
    {
        return switch (format) {
            case ORC -> ORC;
            case PARQUET -> PARQUET;
            case AVRO -> AVRO;
            // Not used as a data file format
            case METADATA -> throw new IllegalArgumentException("Unexpected METADATA file format");
        };
    }

    public String humanName()
    {
        return switch (this) {
            case AVRO -> "Avro";
            case ORC -> "ORC";
            case PARQUET -> "Parquet";
        };
    }
}
