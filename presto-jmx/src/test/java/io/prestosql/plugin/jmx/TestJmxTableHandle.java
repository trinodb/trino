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
package io.prestosql.plugin.jmx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.EquivalenceTester;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jmx.MetadataUtil.TABLE_CODEC;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestJmxTableHandle
{
    public static final List<JmxColumnHandle> COLUMNS = ImmutableList.<JmxColumnHandle>builder()
            .add(new JmxColumnHandle("id", BIGINT))
            .add(new JmxColumnHandle("name", createUnboundedVarcharType()))
            .build();
    public static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("schema", "tableName");
    public static final JmxColumnHandle columnHandle = new JmxColumnHandle("node", createUnboundedVarcharType());
    public static final TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(columnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice("host1"))));

    @Test
    public void testJsonRoundTrip()
    {
        JmxTableHandle table = new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("objectName"), COLUMNS, true, TupleDomain.all());

        String json = TABLE_CODEC.toJson(table);
        JmxTableHandle copy = TABLE_CODEC.fromJson(json);
        assertEquals(copy, table);
    }

    @Test
    public void testEquivalence()
    {
        List<JmxColumnHandle> singleColumn = ImmutableList.of(COLUMNS.get(0));
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, true, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, true, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, false, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, false, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, true, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, true, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, false, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, false, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, true, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, true, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, nodeTupleDomain),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, nodeTupleDomain))
                .check();
    }
}
