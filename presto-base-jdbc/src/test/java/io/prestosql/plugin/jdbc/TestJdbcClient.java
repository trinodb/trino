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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_REAL;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJdbcClient
{
    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    private TestingDatabase database;
    private String catalogName;
    private JdbcClient jdbcClient;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        catalogName = database.getConnection().getCatalog();
        jdbcClient = database.getJdbcClient();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testMetadata()
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        assertTrue(jdbcClient.getSchemaNames(identity).containsAll(ImmutableSet.of("example", "tpch")));
        assertEquals(jdbcClient.getTableNames(identity, Optional.of("example")), ImmutableList.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view")));
        assertEquals(jdbcClient.getTableNames(identity, Optional.of("tpch")), ImmutableList.of(
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("tpch", "orders")));

        SchemaTableName schemaTableName = new SchemaTableName("example", "numbers");
        Optional<JdbcTableHandle> table = jdbcClient.getTableHandle(identity, schemaTableName);
        assertTrue(table.isPresent(), "table is missing");
        assertEquals(table.get().getCatalogName(), catalogName.toUpperCase(ENGLISH));
        assertEquals(table.get().getSchemaName(), "EXAMPLE");
        assertEquals(table.get().getTableName(), "NUMBERS");
        assertEquals(table.get().getSchemaTableName(), schemaTableName);
        assertEquals(jdbcClient.getColumns(session, table.orElse(null)), ImmutableList.of(
                new JdbcColumnHandle("TEXT", JDBC_VARCHAR, VARCHAR, true),
                new JdbcColumnHandle("TEXT_SHORT", JDBC_VARCHAR, createVarcharType(32), true),
                new JdbcColumnHandle("VALUE", JDBC_BIGINT, BIGINT, true)));
    }

    @Test
    public void testMetadataWithSchemaPattern()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "num_ers");
        Optional<JdbcTableHandle> table = jdbcClient.getTableHandle(JdbcIdentity.from(session), schemaTableName);
        assertTrue(table.isPresent(), "table is missing");
        assertEquals(jdbcClient.getColumns(session, table.get()), ImmutableList.of(
                new JdbcColumnHandle("TE_T", JDBC_VARCHAR, VARCHAR, true),
                new JdbcColumnHandle("VA%UE", JDBC_BIGINT, BIGINT, true)));
    }

    @Test
    public void testMetadataWithFloatAndDoubleCol()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "table_with_float_col");
        Optional<JdbcTableHandle> table = jdbcClient.getTableHandle(JdbcIdentity.from(session), schemaTableName);
        assertTrue(table.isPresent(), "table is missing");
        assertEquals(jdbcClient.getColumns(session, table.get()), ImmutableList.of(
                new JdbcColumnHandle("COL1", JDBC_BIGINT, BIGINT, true),
                new JdbcColumnHandle("COL2", JDBC_DOUBLE, DOUBLE, true),
                new JdbcColumnHandle("COL3", JDBC_DOUBLE, DOUBLE, true),
                new JdbcColumnHandle("COL4", JDBC_REAL, REAL, true)));
    }
}
