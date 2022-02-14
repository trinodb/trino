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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.EquivalenceTester;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.jdbc.MetadataUtil.TABLE_CODEC;
import static io.trino.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;

public class TestJdbcTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(TABLE_CODEC, new JdbcTableHandle(new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"));
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JdbcTableHandle(new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle(new SchemaTableName("schema", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle(new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle(new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .addEquivalentGroup(
                        new JdbcTableHandle(new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle(new SchemaTableName("schemaX", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle(new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle(new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .addEquivalentGroup(createNamedHandle())
                .addEquivalentGroup(createQueryBasedHandle())
                .check();
    }

    private JdbcTableHandle createQueryBasedHandle()
    {
        JdbcTypeHandle type = new JdbcTypeHandle(Types.INTEGER, Optional.of("int"), Optional.of(1), Optional.of(2), Optional.of(3), Optional.of(CaseSensitivity.CASE_INSENSITIVE));
        return new JdbcTableHandle(
                new JdbcQueryRelationHandle(
                        new PreparedQuery(
                                "query",
                                ImmutableList.of(new QueryParameter(
                                        type,
                                        IntegerType.INTEGER,
                                        Optional.of(1))))),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.of(1),
                Optional.of(ImmutableList.of(new JdbcColumnHandle("i", type, IntegerType.INTEGER))),
                ImmutableSet.of(),
                0);
    }

    private JdbcTableHandle createNamedHandle()
    {
        JdbcTypeHandle type = new JdbcTypeHandle(Types.INTEGER, Optional.of("int"), Optional.of(1), Optional.of(2), Optional.of(3), Optional.of(CaseSensitivity.CASE_INSENSITIVE));
        return new JdbcTableHandle(
                new JdbcNamedRelationHandle(
                        new SchemaTableName("schema", "table"),
                        new RemoteTableName(Optional.of("catalog"), Optional.of("schema"), "table")),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.of(1),
                Optional.of(ImmutableList.of(new JdbcColumnHandle("i", type, IntegerType.INTEGER))),
                ImmutableSet.of(),
                0);
    }
}
