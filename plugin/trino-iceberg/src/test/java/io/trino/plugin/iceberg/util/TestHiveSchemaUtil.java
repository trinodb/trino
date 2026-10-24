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
package io.trino.plugin.iceberg.util;

import io.trino.metastore.Column;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.toHiveColumns;
import static org.assertj.core.api.Assertions.assertThat;

class TestHiveSchemaUtil
{
    @ParameterizedTest
    @MethodSource("unknownTypes")
    void testUnknownTypeMetastoreRepresentation(Type type, String expected)
    {
        List<Column> columns = toHiveColumns(List.of(Types.NestedField.optional(1, "data", type)));
        assertThat(columns).hasSize(1);
        assertThat(columns.getFirst().getName()).isEqualTo("data");
        assertThat(columns.getFirst().getType().getHiveTypeName().toString()).isEqualTo(expected);
    }

    static Stream<Arguments> unknownTypes()
    {
        return Stream.of(
                Arguments.of(Types.UnknownType.get(), "void"),
                Arguments.of(Types.StructType.of(
                        Types.NestedField.optional(2, "value", Types.IntegerType.get()),
                        Types.NestedField.optional(3, "missing", Types.UnknownType.get())), "struct<value:int,missing:void>"),
                Arguments.of(Types.ListType.ofOptional(2, Types.UnknownType.get()), "array<void>"),
                Arguments.of(Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.UnknownType.get()), "map<string,void>"));
    }
}
