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
package io.trino.plugin.hive.util;

public final class HiveClassNames
{
    public static final String AVRO_CONTAINER_INPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
    public static final String AVRO_CONTAINER_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
    public static final String AVRO_SERDE_CLASS = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
    public static final String COLUMNAR_SERDE_CLASS = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
    public static final String FILE_INPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.FileInputFormat";
    public static final String FILE_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.FileOutputFormat";
    public static final String HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    public static final String HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat";
    public static final String JSON_SERDE_CLASS = "org.apache.hive.hcatalog.data.JsonSerDe";
    public static final String OPENX_JSON_SERDE_CLASS = "org.openx.data.jsonserde.JsonSerDe";
    public static final String LAZY_BINARY_COLUMNAR_SERDE_CLASS = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe";
    public static final String LAZY_SIMPLE_SERDE_CLASS = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    public static final String MAPRED_PARQUET_INPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String MAPRED_PARQUET_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    public static final String OPENCSV_SERDE_CLASS = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    public static final String ORC_INPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    public static final String ORC_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    public static final String ORC_SERDE_CLASS = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
    public static final String PARQUET_HIVE_SERDE_CLASS = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    public static final String RCFILE_INPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    public static final String RCFILE_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat";
    public static final String REGEX_HIVE_SERDE_CLASS = "org.apache.hadoop.hive.serde2.RegexSerDe";
    public static final String SEQUENCEFILE_INPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    public static final String SYMLINK_TEXT_INPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat";
    public static final String TEXT_INPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.TextInputFormat";

    private HiveClassNames() {}
}
