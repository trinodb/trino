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
package io.trino.plugin.hive;

import io.trino.filesystem.local.LocalFileSystem;
import io.trino.hadoop.ConfigurationInstantiator;
import io.trino.plugin.hive.avro.AvroHiveFileUtils;
import io.trino.plugin.hive.avro.TrinoAvroSerDe;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.hive.avro.AvroHiveConstants.TABLE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

public class AvroSchemaGenerationTests
{
    @Test
    public void testOldVsNewSchemaGeneration()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty(TABLE_NAME, "testingTable");
        properties.setProperty(LIST_COLUMNS, "a,b");
        properties.setProperty(LIST_COLUMN_TYPES, Stream.of(HiveType.HIVE_INT, HiveType.HIVE_STRING).map(HiveType::getTypeInfo).map(TypeInfo::toString).collect(Collectors.joining(",")));
        Schema actual = AvroHiveFileUtils.determineSchemaOrThrowException(new LocalFileSystem(Path.of("/")), properties);
        Schema expected = new TrinoAvroSerDe().determineSchemaOrReturnErrorSchema(ConfigurationInstantiator.newEmptyConfiguration(), properties);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testOldVsNewSchemaGenerationWithNested()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty(TABLE_NAME, "testingTable");
        properties.setProperty(LIST_COLUMNS, "a,b");
        properties.setProperty(LIST_COLUMN_TYPES, Stream.of(HiveType.toHiveType(RowType.rowType(RowType.field("a", VarcharType.VARCHAR))), HiveType.HIVE_STRING).map(HiveType::getTypeInfo).map(TypeInfo::toString).collect(Collectors.joining(",")));
        Schema actual = AvroHiveFileUtils.determineSchemaOrThrowException(new LocalFileSystem(Path.of("/")), properties);
        Schema expected = new TrinoAvroSerDe().determineSchemaOrReturnErrorSchema(ConfigurationInstantiator.newEmptyConfiguration(), properties);
        assertThat(actual).isEqualTo(expected);
    }
}
