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

import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.hcatalog.data.JsonSerDe;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.util.HiveClassNames.AVRO_CONTAINER_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_CONTAINER_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.COLUMNAR_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.FILE_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.FILE_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.JSON_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_BINARY_COLUMNAR_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.OPENCSV_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.ORC_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.ORC_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.ORC_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.RCFILE_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.RCFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.SEQUENCEFILE_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.SYMLINK_TEXT_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.TEXT_INPUT_FORMAT_CLASS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveClassNames
{
    @Test
    public void testClassNames()
    {
        assertClassName(AVRO_CONTAINER_INPUT_FORMAT_CLASS, AvroContainerInputFormat.class);
        assertClassName(AVRO_CONTAINER_OUTPUT_FORMAT_CLASS, AvroContainerOutputFormat.class);
        assertClassName(AVRO_SERDE_CLASS, AvroSerDe.class);
        assertClassName(COLUMNAR_SERDE_CLASS, ColumnarSerDe.class);
        assertClassName(FILE_INPUT_FORMAT_CLASS, FileInputFormat.class);
        assertClassName(FILE_OUTPUT_FORMAT_CLASS, FileOutputFormat.class);
        assertClassName(HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS, HiveIgnoreKeyTextOutputFormat.class);
        assertClassName(HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS, HiveSequenceFileOutputFormat.class);
        assertClassName(JSON_SERDE_CLASS, JsonSerDe.class);
        assertClassName(LAZY_BINARY_COLUMNAR_SERDE_CLASS, LazyBinaryColumnarSerDe.class);
        assertClassName(LAZY_SIMPLE_SERDE_CLASS, LazySimpleSerDe.class);
        assertClassName(MAPRED_PARQUET_INPUT_FORMAT_CLASS, MapredParquetInputFormat.class);
        assertClassName(MAPRED_PARQUET_OUTPUT_FORMAT_CLASS, MapredParquetOutputFormat.class);
        assertClassName(OPENCSV_SERDE_CLASS, OpenCSVSerde.class);
        assertClassName(ORC_INPUT_FORMAT_CLASS, OrcInputFormat.class);
        assertClassName(ORC_OUTPUT_FORMAT_CLASS, OrcOutputFormat.class);
        assertClassName(ORC_SERDE_CLASS, OrcSerde.class);
        assertClassName(PARQUET_HIVE_SERDE_CLASS, ParquetHiveSerDe.class);
        assertClassName(RCFILE_INPUT_FORMAT_CLASS, RCFileInputFormat.class);
        assertClassName(RCFILE_OUTPUT_FORMAT_CLASS, RCFileOutputFormat.class);
        assertClassName(SEQUENCEFILE_INPUT_FORMAT_CLASS, SequenceFileInputFormat.class);
        assertClassName(SYMLINK_TEXT_INPUT_FORMAT_CLASS, SymlinkTextInputFormat.class);
        assertClassName(TEXT_INPUT_FORMAT_CLASS, TextInputFormat.class);
    }

    private static void assertClassName(String className, Class<?> clazz)
    {
        assertThat(className).isEqualTo(clazz.getName());
    }
}
