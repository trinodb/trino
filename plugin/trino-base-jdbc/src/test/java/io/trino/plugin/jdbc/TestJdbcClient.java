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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_REAL;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TIMESTAMP;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

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
        database = null;
    }

    @Test
    public void testMetadata()
    {
        assertThat(jdbcClient.getSchemaNames(session).containsAll(ImmutableSet.of("example", "tpch"))).isTrue();
        assertThat(jdbcClient.getTableNames(session, Optional.of("example"))).containsExactly(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "timestamps"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view"));

        assertThat(jdbcClient.getTableNames(session, Optional.of("tpch"))).containsExactly(
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("tpch", "orders"));

        SchemaTableName schemaTableName = new SchemaTableName("example", "numbers");
        Optional<JdbcTableHandle> table = jdbcClient.getTableHandle(session, schemaTableName);
        assertThat(table.isPresent()).withFailMessage("table is missing").isTrue();
        assertThat(table.get().getRequiredNamedRelation().getRemoteTableName().getCatalogName().orElse(null)).isEqualTo(catalogName.toUpperCase(ENGLISH));
        assertThat(table.get().getRequiredNamedRelation().getRemoteTableName().getSchemaName().orElse(null)).isEqualTo("EXAMPLE");
        assertThat(table.get().getRequiredNamedRelation().getRemoteTableName().getTableName()).isEqualTo("NUMBERS");
        assertThat(table.get().getRequiredNamedRelation().getSchemaTableName()).isEqualTo(schemaTableName);
        assertThat(jdbcClient.getColumns(session, table.orElse(null))).containsExactly(
                new JdbcColumnHandle("TEXT", JDBC_VARCHAR, VARCHAR),
                new JdbcColumnHandle("TEXT_SHORT", JDBC_VARCHAR, createVarcharType(32)),
                new JdbcColumnHandle("VALUE", JDBC_BIGINT, BIGINT));
    }

    @Test
    public void testMetadataWithSchemaPattern()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "num_ers");
        Optional<JdbcTableHandle> table = jdbcClient.getTableHandle(session, schemaTableName);
        assertThat(table.isPresent()).withFailMessage("table is missing").isTrue();
        assertThat(jdbcClient.getColumns(session, table.get())).containsExactly(
                new JdbcColumnHandle("TE_T", JDBC_VARCHAR, VARCHAR),
                new JdbcColumnHandle("VA%UE", JDBC_BIGINT, BIGINT));
    }

    @Test
    public void testMetadataWithFloatAndDoubleCol()
    {
        SchemaTableName schemaTableName = new SchemaTableName("exa_ple", "table_with_float_col");
        Optional<JdbcTableHandle> table = jdbcClient.getTableHandle(session, schemaTableName);
        assertThat(table.isPresent()).withFailMessage("table is missing").isTrue();
        assertThat(jdbcClient.getColumns(session, table.get())).containsExactly(
                new JdbcColumnHandle("COL1", JDBC_BIGINT, BIGINT),
                new JdbcColumnHandle("COL2", JDBC_DOUBLE, DOUBLE),
                new JdbcColumnHandle("COL3", JDBC_DOUBLE, DOUBLE),
                new JdbcColumnHandle("COL4", JDBC_REAL, REAL));
    }

    @Test
    public void testMetadataWithTimestampCol()
    {
        SchemaTableName schemaTableName = new SchemaTableName("example", "timestamps");
        Optional<JdbcTableHandle> table = jdbcClient.getTableHandle(session, schemaTableName);
        assertThat(table.isPresent()).withFailMessage("table is missing").isTrue();
        assertThat(jdbcClient.getColumns(session, table.get())).containsExactly(
                new JdbcColumnHandle("TS_3", JDBC_TIMESTAMP, TIMESTAMP_MILLIS),
                new JdbcColumnHandle("TS_6", JDBC_TIMESTAMP, TIMESTAMP_MICROS),
                new JdbcColumnHandle("TS_9", JDBC_TIMESTAMP, TIMESTAMP_NANOS));
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = "test schema";
        jdbcClient.createSchema(session, schemaName);
        assertThat(jdbcClient.getSchemaNames(session)).contains(schemaName);
        jdbcClient.dropSchema(session, schemaName);
        assertThat(jdbcClient.getSchemaNames(session)).doesNotContain(schemaName);
    }

    @Test
    public void testRenameTable()
    {
        String schemaName = "test_schema";
        SchemaTableName oldTable = new SchemaTableName(schemaName, "foo");
        SchemaTableName newTable = new SchemaTableName(schemaName, "bar");
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                oldTable,
                ImmutableList.of(new ColumnMetadata("text", VARCHAR)));

        jdbcClient.createSchema(session, schemaName);
        jdbcClient.createTable(session, tableMetadata);
        jdbcClient.renameTable(session, jdbcClient.getTableHandle(session, oldTable).get(), newTable);
        jdbcClient.dropTable(session, jdbcClient.getTableHandle(session, newTable).get());
        jdbcClient.dropSchema(session, schemaName);
        assertThat(jdbcClient.getTableNames(session, Optional.empty()))
                .doesNotContain(oldTable)
                .doesNotContain(newTable);
        assertThat(jdbcClient.getSchemaNames(session)).doesNotContain(schemaName);
    }
}
