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
package io.trino.plugin.hive.avro;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.type.RowType;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.avro.AvroHiveConstants.TABLE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAvroSchemaGeneration
{
    @Test
    public void testOldVsNewSchemaGeneration()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(TABLE_NAME, "testingTable")
                .put(LIST_COLUMNS, "a,b")
                .put(LIST_COLUMN_TYPES, Stream.of(HiveType.HIVE_INT, HIVE_STRING).map(HiveType::getTypeInfo).map(TypeInfo::toString).collect(Collectors.joining(",")))
                .buildOrThrow();
        Schema actual = AvroHiveFileUtils.determineSchemaOrThrowException(new LocalFileSystem(Path.of("/")), properties);
        Schema expected = AvroSerdeUtils.determineSchemaOrThrowException(new Configuration(false), toProperties(properties));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testOldVsNewSchemaGenerationWithNested()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(TABLE_NAME, "testingTable")
                .put(LIST_COLUMNS, "a,b")
                .put(LIST_COLUMN_TYPES, toHiveType(RowType.rowType(RowType.field("a", VARCHAR))) + "," + HIVE_STRING)
                .buildOrThrow();
        Schema actual = AvroHiveFileUtils.determineSchemaOrThrowException(new LocalFileSystem(Path.of("/")), properties);

        Schema expected = AvroSerdeUtils.determineSchemaOrThrowException(new Configuration(false), toProperties(properties));
        assertThat(actual).isEqualTo(expected);
    }

    private static Properties toProperties(Map<String, String> properties)
    {
        Properties hiveProperties = new Properties();
        hiveProperties.putAll(properties);
        return hiveProperties;
    }
}
