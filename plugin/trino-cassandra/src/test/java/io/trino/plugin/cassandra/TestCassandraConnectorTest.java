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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.Bytes;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.datastax.oss.driver.api.core.data.ByteUtils.toHexString;
import static com.google.common.io.BaseEncoding.base16;
import static io.trino.plugin.cassandra.TestCassandraTable.clusterColumn;
import static io.trino.plugin.cassandra.TestCassandraTable.columnsValue;
import static io.trino.plugin.cassandra.TestCassandraTable.generalColumn;
import static io.trino.plugin.cassandra.TestCassandraTable.partitionColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.QueryAssertions.assertContainsEventually;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestCassandraConnectorTest
        extends BaseConnectorTest
{
    private static final ZonedDateTime TIMESTAMP_VALUE = ZonedDateTime.of(1970, 1, 1, 3, 4, 5, 0, ZoneId.of("UTC"));

    private CassandraSession session;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CassandraServer server = closeAfterClass(new CassandraServer());
        session = server.getSession();
        return CassandraQueryRunner.builder(server)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterAll
    public void cleanUp()
    {
        session.close();
        session = null;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("Cassandra connector does not support column default values");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp")
                || typeName.equals("decimal(5,3)")
                || typeName.equals("decimal(15,3)")
                || typeName.equals("char(3)")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }
        if (typeName.equals("time(6)") ||
                typeName.equals("timestamp(6)") ||
                typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected String dataMappingTableName(String trinoTypeName)
    {
        return "tmp_trino_" + System.nanoTime();
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).result().matches(getDescribeOrdersResult());
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE cassandra.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessage("Unsupported type: char(3)");
    }

    @Test
    @Override // Override because some tests (e.g. testKeyspaceNameAmbiguity) cause table listing failure
    public void testShowInformationSchemaTables()
    {
        executeExclusively(super::testShowInformationSchemaTables);
    }

    @Test
    @Override // Override because some tests (e.g. testKeyspaceNameAmbiguity, testNativeQueryCaseSensitivity) cause column listing failure
    public void testSelectInformationSchemaColumns()
    {
        executeExclusively(super::testSelectInformationSchemaColumns);
    }

    @Test
    public void testPushdownUuidPartitionKeyPredicate()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_pushdown_uuid_partition_key",
                ImmutableList.of(partitionColumn("col_uuid", "uuid"), generalColumn("col_text", "text")),
                ImmutableList.of("00000000-0000-0000-0000-000000000001, 'Trino'"))) {
            assertThat(query(format("SELECT col_text FROM %s WHERE col_uuid = UUID '00000000-0000-0000-0000-000000000001'", testCassandraTable.getTableName())))
                    .matches("VALUES CAST('Trino' AS varchar)");
        }
    }

    @Test
    public void testSelectWithFilterOnPartitioningKey()
    {
        try (TestCassandraTable table = testTable(
                "table_filter_on_partition_key",
                ImmutableList.of(generalColumn("id", "int"), partitionColumn("part", "int")),
                ImmutableList.of("1, 10", "2, 20"))) {
            // predicate on partition column
            assertThat(query("SELECT id FROM " + table.getTableName() + " WHERE part > 10"))
                    .matches("VALUES 2");

            // predicate on non-partition column
            assertThat(query("SELECT id FROM " + table.getTableName() + " WHERE id = 1"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getTableName() + " WHERE id < 2"))
                    .matches("VALUES 1");
        }
    }

    @Test
    public void testPushdownAllTypesPartitionKeyPredicate()
    {
        // TODO partition key predicate pushdown for decimal types does not work https://github.com/trinodb/trino/issues/10927
        try (TestCassandraTable testCassandraTable = testTable(
                "table_pushdown_all_types_partition_key",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        partitionColumn("typeuuid", "uuid"),
                        partitionColumn("typetinyint", "tinyint"),
                        partitionColumn("typesmallint", "smallint"),
                        partitionColumn("typeinteger", "int"),
                        partitionColumn("typelong", "bigint"),
                        generalColumn("typebytes", "blob"),
                        partitionColumn("typedate", "date"),
                        partitionColumn("typetime", "time"),
                        partitionColumn("typetimestamp", "timestamp"),
                        partitionColumn("typeansi", "ascii"),
                        partitionColumn("typeboolean", "boolean"),
                        generalColumn("typedecimal", "decimal"),
                        partitionColumn("typedouble", "double"),
                        partitionColumn("typefloat", "float"),
                        partitionColumn("typeinet", "inet"),
                        partitionColumn("typevarchar", "varchar"),
                        generalColumn("typevarint", "varint"),
                        partitionColumn("typetimeuuid", "timeuuid"),
                        generalColumn("typelist", "frozen <list<text>>"),
                        generalColumn("typemap", "frozen <map<int, bigint>>"),
                        generalColumn("typeset", "frozen <set<boolean>>")),
                ImmutableList.of("" +
                        "'key 7', " +
                        "00000000-0000-0000-0000-000000000007, " +
                        "7, " +
                        "7, " +
                        "7, " +
                        "1007, " +
                        "0x00000007, " +
                        "'1970-01-01', " +
                        "'03:04:05.123456789', " +
                        "'1970-01-01 03:04:05.000+0000', " +
                        "'ansi 7', " +
                        "false, " +
                        "128.0, " +
                        "16384.0, " +
                        "2097152.0, " +
                        "'127.0.0.1', " +
                        "'varchar 7', " +
                        "10000000, " +
                        "d2177dd0-eaa2-11de-a572-001b779c76e7, " +
                        "['list-value-17', 'list-value-27'], " +
                        "{7:8, 9:10}, " +
                        "{false, true}"))) {
            String sql = "SELECT *" +
                    " FROM " + testCassandraTable.getTableName() +
                    " WHERE key = 'key 7'" +
                    " AND typeuuid = UUID '00000000-0000-0000-0000-000000000007'" +
                    " AND typetinyint = 7" +
                    " AND typesmallint = 7" +
                    " AND typeinteger = 7" +
                    " AND typelong = 1007" +
                    " AND typedate = DATE '1970-01-01'" +
                    " AND typetime = TIME '03:04:05.123456789'" +
                    " AND typetimestamp = TIMESTAMP '1970-01-01 03:04:05Z'" +
                    " AND typeansi = 'ansi 7'" +
                    " AND typeboolean = false" +
                    " AND typedouble = 16384.0" +
                    " AND typefloat = REAL '2097152.0'" +
                    " AND typeinet = IPADDRESS '127.0.0.1'" +
                    " AND typevarchar = 'varchar 7'" +
                    " AND typetimeuuid = UUID 'd2177dd0-eaa2-11de-a572-001b779c76e7'" +
                    "";
            MaterializedResult result = computeActual(sql);

            assertThat(result.getRowCount()).isEqualTo(1);
        }
    }

    @Test
    public void testPartitionPushdownsWithNotMatchingPredicate()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "partition_not_pushed_down_keys",
                ImmutableList.of(partitionColumn("id", "varchar"), generalColumn("trino_filter_col", "int")),
                ImmutableList.of("'2', 0"))) {
            String sql = "SELECT 1 FROM " + testCassandraTable.getTableName() + " WHERE id = '1' AND trino_filter_col = 0";

            assertThat(computeActual(sql).getMaterializedRows()).isEmpty();
        }
    }

    @Test
    public void testPartitionKeyPredicate()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_all_types_partition_key",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        partitionColumn("typeuuid", "uuid"),
                        partitionColumn("typetinyint", "tinyint"),
                        partitionColumn("typesmallint", "smallint"),
                        partitionColumn("typeinteger", "int"),
                        partitionColumn("typelong", "bigint"),
                        partitionColumn("typebytes", "blob"),
                        partitionColumn("typedate", "date"),
                        partitionColumn("typetimestamp", "timestamp"),
                        partitionColumn("typeansi", "ascii"),
                        partitionColumn("typeboolean", "boolean"),
                        partitionColumn("typedecimal", "decimal"),
                        partitionColumn("typedouble", "double"),
                        partitionColumn("typefloat", "float"),
                        partitionColumn("typeinet", "inet"),
                        partitionColumn("typevarchar", "varchar"),
                        partitionColumn("typevarint", "varint"),
                        partitionColumn("typetimeuuid", "timeuuid"),
                        partitionColumn("typelist", "frozen <list<text>>"),
                        partitionColumn("typemap", "frozen <map<int, bigint>>"),
                        partitionColumn("typeset", "frozen <set<boolean>>")),
                ImmutableList.of("" +
                        "'key 7', " +
                        "00000000-0000-0000-0000-000000000007, " +
                        "7, " +
                        "7, " +
                        "7, " +
                        "1007, " +
                        "0x00000007, " +
                        "'1970-01-01', " +
                        "'1970-01-01 03:04:05.000+0000', " +
                        "'ansi 7', " +
                        "false, " +
                        "128.0, " +
                        "16384.0, " +
                        "2097152.0, " +
                        "'127.0.0.1', " +
                        "'varchar 7', " +
                        "10000000, " +
                        "d2177dd0-eaa2-11de-a572-001b779c76e7, " +
                        "['list-value-17', 'list-value-27'], " +
                        "{7:8, 9:10}, " +
                        "{false, true}"))) {
            String sql = "SELECT *" +
                    " FROM " + testCassandraTable.getTableName() +
                    " WHERE key = 'key 7'" +
                    " AND typeuuid = UUID '00000000-0000-0000-0000-000000000007'" +
                    " AND typetinyint = 7" +
                    " AND typesmallint = 7" +
                    " AND typeinteger = 7" +
                    " AND typelong = 1007" +
                    " AND typebytes = from_hex('" + base16().encode(Ints.toByteArray(7)) + "')" +
                    " AND typedate = DATE '1970-01-01'" +
                    " AND typetimestamp = TIMESTAMP '1970-01-01 03:04:05Z'" +
                    " AND typeansi = 'ansi 7'" +
                    " AND typeboolean = false" +
                    " AND typedecimal = 128.0" +
                    " AND typedouble = 16384.0" +
                    " AND typefloat = REAL '2097152.0'" +
                    " AND typeinet = IPADDRESS '127.0.0.1'" +
                    " AND typevarchar = 'varchar 7'" +
                    " AND typevarint = '10000000'" +
                    " AND typetimeuuid = UUID 'd2177dd0-eaa2-11de-a572-001b779c76e7'" +
                    " AND typelist = '[\"list-value-17\",\"list-value-27\"]'" +
                    " AND typemap = '{7:8,9:10}'" +
                    " AND typeset = '[false,true]'" +
                    "";
            MaterializedResult result = computeActual(sql);

            assertThat(result.getRowCount()).isEqualTo(1);
        }
    }

    @Test
    public void testTimestampPartitionKey()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "test_timestamp",
                ImmutableList.of(partitionColumn("c1", "timestamp")),
                ImmutableList.of("'2017-04-01T11:21:59.001+0000'"))) {
            String sql = format(
                    "SELECT * " +
                            "FROM %s " +
                            "WHERE c1 = TIMESTAMP '2017-04-01 11:21:59.001 UTC'", testCassandraTable.getTableName());
            MaterializedResult result = computeActual(sql);

            assertThat(result.getRowCount()).isEqualTo(1);
        }
    }

    @Test
    public void testSelect()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_all_types",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        generalColumn("typeuuid", "uuid"),
                        generalColumn("typetinyint", "tinyint"),
                        generalColumn("typesmallint", "smallint"),
                        generalColumn("typeinteger", "int"),
                        generalColumn("typelong", "bigint"),
                        generalColumn("typebytes", "blob"),
                        generalColumn("typedate", "date"),
                        generalColumn("typetimestamp", "timestamp"),
                        generalColumn("typeansi", "ascii"),
                        generalColumn("typeboolean", "boolean"),
                        generalColumn("typedecimal", "decimal"),
                        generalColumn("typedouble", "double"),
                        generalColumn("typefloat", "float"),
                        generalColumn("typeinet", "inet"),
                        generalColumn("typevarchar", "varchar"),
                        generalColumn("typevarint", "varint"),
                        generalColumn("typetimeuuid", "timeuuid"),
                        generalColumn("typelist", "frozen <list<text>>"),
                        generalColumn("typemap", "frozen <map<int, bigint>>"),
                        generalColumn("typeset", "frozen <set<boolean>>")),
                columnsValue(9, ImmutableList.of(
                        rowNumber -> format("'key %d'", rowNumber),
                        rowNumber -> format("00000000-0000-0000-0000-%012d", rowNumber),
                        String::valueOf,
                        String::valueOf,
                        String::valueOf,
                        rowNumber -> String.valueOf(rowNumber + 1000),
                        rowNumber -> toHexString(ByteBuffer.wrap(Ints.toByteArray(rowNumber)).asReadOnlyBuffer()),
                        rowNumber -> format("'%s'", DateTimeFormatter.ofPattern("uuuu-MM-dd").format(TIMESTAMP_VALUE)),
                        rowNumber -> format("'%s'", DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSZ").format(TIMESTAMP_VALUE)),
                        rowNumber -> format("'ansi %d'", rowNumber),
                        rowNumber -> String.valueOf(rowNumber % 2 == 0),
                        rowNumber -> new BigDecimal(Math.pow(2, rowNumber)).toString(),
                        rowNumber -> String.valueOf(Math.pow(4, rowNumber)),
                        rowNumber -> String.valueOf((float) Math.pow(8, rowNumber)),
                        rowNumber -> format("'%s'", "127.0.0.1"),
                        rowNumber -> format("'varchar %d'", rowNumber),
                        rowNumber -> BigInteger.TEN.pow(rowNumber).toString(),
                        rowNumber -> format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber),
                        rowNumber -> format("['list-value-1%d', 'list-value-2%d']", rowNumber, rowNumber),
                        rowNumber -> format("{%d:%d, %d:%d}", rowNumber, rowNumber + 1, rowNumber + 2, rowNumber + 3),
                        rowNumber -> format("{false, true}"))))) {
            assertSelect(testCassandraTable.getTableName());
        }

        try (TestCassandraTable testCassandraTable = testTable(
                "table_all_types_partition_key",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        partitionColumn("typeuuid", "uuid"),
                        partitionColumn("typetinyint", "tinyint"),
                        partitionColumn("typesmallint", "smallint"),
                        partitionColumn("typeinteger", "int"),
                        partitionColumn("typelong", "bigint"),
                        partitionColumn("typebytes", "blob"),
                        partitionColumn("typedate", "date"),
                        partitionColumn("typetimestamp", "timestamp"),
                        partitionColumn("typeansi", "ascii"),
                        partitionColumn("typeboolean", "boolean"),
                        partitionColumn("typedecimal", "decimal"),
                        partitionColumn("typedouble", "double"),
                        partitionColumn("typefloat", "float"),
                        partitionColumn("typeinet", "inet"),
                        partitionColumn("typevarchar", "varchar"),
                        partitionColumn("typevarint", "varint"),
                        partitionColumn("typetimeuuid", "timeuuid"),
                        partitionColumn("typelist", "frozen <list<text>>"),
                        partitionColumn("typemap", "frozen <map<int, bigint>>"),
                        partitionColumn("typeset", "frozen <set<boolean>>")),
                columnsValue(9, ImmutableList.of(
                        rowNumber -> format("'key %d'", rowNumber),
                        rowNumber -> format("00000000-0000-0000-0000-%012d", rowNumber),
                        String::valueOf,
                        String::valueOf,
                        String::valueOf,
                        rowNumber -> String.valueOf(rowNumber + 1000),
                        rowNumber -> toHexString(ByteBuffer.wrap(Ints.toByteArray(rowNumber))),
                        rowNumber -> format("'%s'", DateTimeFormatter.ofPattern("uuuu-MM-dd").format(TIMESTAMP_VALUE)),
                        rowNumber -> format("'%s'", DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSZ").format(TIMESTAMP_VALUE)),
                        rowNumber -> format("'ansi %d'", rowNumber),
                        rowNumber -> String.valueOf(rowNumber % 2 == 0),
                        rowNumber -> new BigDecimal(Math.pow(2, rowNumber)).toString(),
                        rowNumber -> String.valueOf(Math.pow(4, rowNumber)),
                        rowNumber -> String.valueOf((float) Math.pow(8, rowNumber)),
                        rowNumber -> format("'%s'", "127.0.0.1"),
                        rowNumber -> format("'varchar %d'", rowNumber),
                        rowNumber -> BigInteger.TEN.pow(rowNumber).toString(),
                        rowNumber -> format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber),
                        rowNumber -> format("['list-value-1%d', 'list-value-2%d']", rowNumber, rowNumber),
                        rowNumber -> format("{%d:%d, %d:%d}", rowNumber, rowNumber + 1, rowNumber + 2, rowNumber + 3),
                        rowNumber -> format("{false, true}"))))) {
            assertSelect(testCassandraTable.getTableName());
        }
    }

    @Test
    public void testInsertToTableWithHiddenId()
    {
        assertUpdate("DROP TABLE IF EXISTS test_create_table");
        assertUpdate("CREATE TABLE test_create_table (col1 integer)");
        assertUpdate("INSERT INTO test_create_table VALUES (12345)", 1);
        assertQuery("SELECT * FROM test_create_table", "VALUES (12345)");
        assertUpdate("DROP TABLE test_create_table");
    }

    @Test
    void testInsertIntoTupleType()
    {
        try (TestCassandraTable table = testTable(
                "insert_tuple_table",
                ImmutableList.of(partitionColumn("key", "int"), generalColumn("value", "frozen<tuple<int, text, float>>")),
                ImmutableList.of())) {
            assertQueryFails(
                    format("INSERT INTO %s (key, value) VALUES (1, ROW(1, 'text-1', 1.11))", table.getTableName()),
                    "\\QUnsupported column type: row(integer, varchar, real)");
        }
    }

    @Test
    void testInsertIntoValuesToCassandraMaterializedView()
    {
        String materializedViewName = "test_insert_into_mv" + randomNameSuffix();
        onCassandra("CREATE MATERIALIZED VIEW tpch." + materializedViewName + " AS " +
                "SELECT * FROM tpch.nation " +
                "WHERE nationkey IS NOT NULL " +
                "PRIMARY KEY (id, nationkey)");

        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), VARCHAR)
                .row(materializedViewName)
                .build(), new Duration(1, MINUTES));

        assertQueryFails(
                "INSERT INTO tpch.%s (nationkey) VALUES (null)".formatted(materializedViewName),
                "Inserting into materialized views not yet supported");
        assertQueryFails(
                "DROP TABLE tpch." + materializedViewName,
                "Dropping materialized views not yet supported");

        onCassandra("DROP MATERIALIZED VIEW tpch." + materializedViewName);
    }

    @Test
    void testInvalidTable()
    {
        String tableName = "cassandra.tpch.bogus";
        assertQueryFails("SELECT * FROM " + tableName, ".* Table '%s' does not exist".formatted(tableName));
    }

    @Test
    void testInvalidSchema()
    {
        assertQueryFails(
                "SELECT * FROM cassandra.does_not_exist.bogus",
                ".* Schema 'does_not_exist' does not exist");
    }

    @Test
    void testInvalidColumn()
    {
        assertQueryFails("SELECT bogus FROM nation", ".* Column 'bogus' cannot be resolved");
    }

    @Test
    public void testCreateTableAs()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_all_types",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        generalColumn("typeuuid", "uuid"),
                        generalColumn("typetinyint", "tinyint"),
                        generalColumn("typesmallint", "smallint"),
                        generalColumn("typeinteger", "int"),
                        generalColumn("typelong", "bigint"),
                        generalColumn("typebytes", "blob"),
                        generalColumn("typedate", "date"),
                        generalColumn("typetimestamp", "timestamp"),
                        generalColumn("typeansi", "ascii"),
                        generalColumn("typeboolean", "boolean"),
                        generalColumn("typedecimal", "decimal"),
                        generalColumn("typedouble", "double"),
                        generalColumn("typefloat", "float"),
                        generalColumn("typeinet", "inet"),
                        generalColumn("typevarchar", "varchar"),
                        generalColumn("typevarint", "varint"),
                        generalColumn("typetimeuuid", "timeuuid"),
                        generalColumn("typelist", "frozen <list<text>>"),
                        generalColumn("typemap", "frozen <map<int, bigint>>"),
                        generalColumn("typeset", "frozen <set<boolean>>")),
                columnsValue(9, ImmutableList.of(
                        rowNumber -> format("'key %d'", rowNumber),
                        rowNumber -> format("00000000-0000-0000-0000-%012d", rowNumber),
                        String::valueOf,
                        String::valueOf,
                        String::valueOf,
                        rowNumber -> String.valueOf(rowNumber + 1000),
                        rowNumber -> toHexString(ByteBuffer.wrap(Ints.toByteArray(rowNumber))),
                        rowNumber -> format("'%s'", DateTimeFormatter.ofPattern("uuuu-MM-dd").format(TIMESTAMP_VALUE)),
                        rowNumber -> format("'%s'", DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSZ").format(TIMESTAMP_VALUE)),
                        rowNumber -> format("'ansi %d'", rowNumber),
                        rowNumber -> String.valueOf(rowNumber % 2 == 0),
                        rowNumber -> new BigDecimal(Math.pow(2, rowNumber)).toString(),
                        rowNumber -> String.valueOf(Math.pow(4, rowNumber)),
                        rowNumber -> String.valueOf((float) Math.pow(8, rowNumber)),
                        rowNumber -> format("'%s'", "127.0.0.1"),
                        rowNumber -> format("'varchar %d'", rowNumber),
                        rowNumber -> BigInteger.TEN.pow(rowNumber).toString(),
                        rowNumber -> format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber),
                        rowNumber -> format("['list-value-1%d', 'list-value-2%d']", rowNumber, rowNumber),
                        rowNumber -> format("{%d:%d, %d:%d}", rowNumber, rowNumber + 1, rowNumber + 2, rowNumber + 3),
                        rowNumber -> format("{false, true}"))))) {
            assertUpdate("DROP TABLE IF EXISTS table_all_types_copy");
            assertUpdate("CREATE TABLE table_all_types_copy AS SELECT * FROM " + testCassandraTable.getTableName(), 9);
            assertSelect("table_all_types_copy");
            assertUpdate("DROP TABLE table_all_types_copy");
        }
    }

    @Test
    public void testIdentifiers()
    {
        session.execute("DROP KEYSPACE IF EXISTS \"_keyspace\"");
        session.execute("CREATE KEYSPACE \"_keyspace\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> computeActual("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("_keyspace")
                .build(), new Duration(1, MINUTES));

        assertUpdate("CREATE TABLE _keyspace._table AS SELECT 1 AS \"_col\", 2 AS \"2col\"", 1);
        assertQuery("SHOW TABLES FROM cassandra._keyspace", "VALUES ('_table')");
        assertQuery("SELECT * FROM cassandra._keyspace._table", "VALUES (1, 2)");
        assertUpdate("DROP TABLE cassandra._keyspace._table");

        session.execute("DROP KEYSPACE \"_keyspace\"");
    }

    @Test
    public void testClusteringPredicates()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_clustering_keys",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        clusterColumn("clust_one", "text"),
                        clusterColumn("clust_two", "text"),
                        clusterColumn("clust_three", "text"),
                        generalColumn("data", "text")),
                columnsValue(9, ImmutableList.of(
                        rowNumber -> format("'key_%d'", rowNumber),
                        rowNumber -> "'clust_one'",
                        rowNumber -> format("'clust_two_%d'", rowNumber),
                        rowNumber -> format("'clust_three_%d'", rowNumber),
                        rowNumber -> "null")))) {
            String sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one!='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(0);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key IN ('key_1','key_2','key_3','key_4') AND clust_one='clust_one' AND clust_two>'clust_two_1'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(3);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND " +
                    "((clust_two='clust_two_1') OR (clust_two='clust_two_2'))";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND " +
                    "((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_three='clust_three_1'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
        }
    }

    @Test
    public void testMultiplePartitionClusteringPredicates()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_multi_partition_clustering_keys",
                ImmutableList.of(
                        partitionColumn("partition_one", "text"),
                        partitionColumn("partition_two", "text"),
                        clusterColumn("clust_one", "text"),
                        clusterColumn("clust_two", "text"),
                        clusterColumn("clust_three", "text"),
                        generalColumn("data", "text")),
                columnsValue(9, ImmutableList.of(
                        rowNumber -> format("'partition_one_%d'", rowNumber),
                        rowNumber -> format("'partition_two_%d'", rowNumber),
                        rowNumber -> "'clust_one'",
                        rowNumber -> format("'clust_two_%d'", rowNumber),
                        rowNumber -> format("'clust_three_%d'", rowNumber),
                        rowNumber -> "null")))) {
            String partitionInPredicates = " partition_one IN ('partition_one_1','partition_one_2') AND partition_two IN ('partition_two_1','partition_two_2') ";
            String sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE " + partitionInPredicates + " AND clust_one='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one!='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(0);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE " +
                    "partition_one IN ('partition_one_1','partition_one_2','partition_one_3','partition_one_4') AND " +
                    "partition_two IN ('partition_two_1','partition_two_2','partition_two_3','partition_two_4') AND " +
                    "clust_one='clust_one' AND clust_two>'clust_two_1'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(3);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND " +
                    "((clust_two='clust_two_1') OR (clust_two='clust_two_2'))";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND " +
                    "((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND clust_three='clust_three_1'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
        }
    }

    @Test
    public void testClusteringKeyOnlyPushdown()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_clustering_keys",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        clusterColumn("clust_one", "text"),
                        clusterColumn("clust_two", "text"),
                        clusterColumn("clust_three", "text"),
                        generalColumn("data", "text")),
                columnsValue(9, ImmutableList.of(
                        rowNumber -> format("'key_%d'", rowNumber),
                        rowNumber -> "'clust_one'",
                        rowNumber -> format("'clust_two_%d'", rowNumber),
                        rowNumber -> format("'clust_three_%d'", rowNumber),
                        rowNumber -> "null")))) {
            String sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(9);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two='clust_two_2'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
        }

        try (TestCassandraTable testCassandraTable = testTable(
                "table_clustering_keys",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        clusterColumn("clust_one", "text"),
                        clusterColumn("clust_two", "text"),
                        clusterColumn("clust_three", "text"),
                        generalColumn("data", "text")),
                columnsValue(1000, ImmutableList.of(
                        rowNumber -> format("'key_%d'", rowNumber),
                        rowNumber -> "'clust_one'",
                        rowNumber -> format("'clust_two_%d'", rowNumber),
                        rowNumber -> format("'clust_three_%d'", rowNumber),
                        rowNumber -> "null")))) {
            // below test cases are needed to verify clustering key pushdown with unpartitioned table
            // for the smaller table (<200 partitions by default) connector fetches all the partitions id
            // and the partitioned patch is being followed
            String sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two='clust_two_2'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two > 'clust_two_998'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two > 'clust_two_997' AND clust_two < 'clust_two_999'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_998'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(0);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three < 'clust_three_3'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_1' AND clust_three < 'clust_three_3'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two < 'clust_two_2'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_997','clust_two_998','clust_two_999') AND clust_two > 'clust_two_998'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two = 'clust_two_2'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
        }
    }

    @Test
    public void testNotEqualPredicateOnClusteringColumn()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_clustering_keys_inequality",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        clusterColumn("clust_one", "text"),
                        clusterColumn("clust_two", "int"),
                        clusterColumn("clust_three", "timestamp"),
                        generalColumn("data", "text")),
                columnsValue(4, ImmutableList.of(
                        rowNumber -> "'key_1'",
                        rowNumber -> "'clust_one'",
                        rowNumber -> format("%d", rowNumber),
                        rowNumber -> format("%d", Timestamp.from(TIMESTAMP_VALUE.toInstant()).getTime() + rowNumber * 10),
                        rowNumber -> "null")))) {
            String sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one != 'clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(0);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two != 2";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(3);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two >= 2 AND clust_two != 3";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two > 2 AND clust_two != 3";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
        }
    }

    @Test
    public void testClusteringKeyPushdownInequality()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_clustering_keys_inequality",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        clusterColumn("clust_one", "text"),
                        clusterColumn("clust_two", "int"),
                        clusterColumn("clust_three", "timestamp"),
                        generalColumn("data", "text")),
                columnsValue(4, ImmutableList.of(
                        rowNumber -> "'key_1'",
                        rowNumber -> "'clust_one'",
                        rowNumber -> format("%d", rowNumber),
                        rowNumber -> format("%d", Timestamp.from(TIMESTAMP_VALUE.toInstant()).getTime() + rowNumber * 10),
                        rowNumber -> "null")))) {
            String sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(4);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1970-01-01 03:04:05.020Z'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1970-01-01 03:04:05.010Z'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(0);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2)";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two > 1 AND clust_two < 3";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three >= timestamp '1970-01-01 03:04:05.010Z' AND clust_three <= timestamp '1970-01-01 03:04:05.020Z'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2) AND clust_three >= timestamp '1970-01-01 03:04:05.010Z' AND clust_three <= timestamp '1970-01-01 03:04:05.020Z'";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(2);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two < 2";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two > 2";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
            sql = "SELECT * FROM " + testCassandraTable.getTableName() + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two = 2";
            assertThat(computeActual(sql).getRowCount()).isEqualTo(1);
        }
    }

    @Test
    void testMultiColumnKey()
    {
        try (TestCassandraTable table = testTable(
                "test_multi_column_key",
                ImmutableList.of(
                        partitionColumn("user_id", "text"),
                        partitionColumn("key", "text"),
                        partitionColumn("updated_at", "timestamp"),
                        generalColumn("value", "text")),
                ImmutableList.of(
                        "'Alice', 'a1', '2015-01-01 01:01:01', 'Test value 1'",
                        "'Bob', 'b1', '2014-02-02 03:04:05', 'Test value 2'"))) {
            // equality filter on clustering key
            assertQuery("SELECT value FROM " + table.getTableName() + " WHERE key = 'a1'", "VALUES 'Test value 1'");

            // equality filter on primary and clustering keys
            assertQuery("SELECT value FROM " + table.getTableName() + " WHERE user_id = 'Alice' and key = 'a1' and updated_at = TIMESTAMP '2015-01-01 01:01:01Z'",
                    "VALUES 'Test value 1'");

            // mixed filter on primary and clustering keys
            assertQuery("SELECT value FROM " + table.getTableName() + " WHERE user_id = 'Alice' and key < 'b' and updated_at >= TIMESTAMP '2015-01-01 01:01:01Z'",
                    "VALUES 'Test value 1'");

            // filter on primary key doesn't match
            assertQueryReturnsEmptyResult("SELECT value FROM " + table.getTableName() + " WHERE user_id = 'George'");

            // filter on prefix of clustering key
            assertQuery("SELECT value FROM " + table.getTableName() + " WHERE user_id = 'Bob' and key = 'b1'",
                    "VALUES 'Test value 2'");

            // filter on second clustering key
            assertQuery("SELECT value FROM " + table.getTableName() + " WHERE user_id = 'Bob' and updated_at = TIMESTAMP '2014-02-02 03:04:05Z'",
                    "VALUES 'Test value 2'");
        }
    }

    @Test
    public void testSelectWithSecondaryIndex()
    {
        testSelectWithSecondaryIndex(true);
        testSelectWithSecondaryIndex(false);
    }

    private void testSelectWithSecondaryIndex(boolean withClusteringKey)
    {
        String table = "test_cluster_key_" + randomNameSuffix();

        if (withClusteringKey) {
            onCassandra("CREATE TABLE tpch." + table + "(pk1_col text, pk2_col text, idx1_col text, idx2_col text, cluster_col tinyint, PRIMARY KEY ((pk1_col, pk2_col), cluster_col)) WITH CLUSTERING ORDER BY (cluster_col ASC)");
        }
        else {
            onCassandra("CREATE TABLE tpch." + table + "(pk1_col text, pk2_col text, idx1_col text, idx2_col text, cluster_col tinyint, PRIMARY KEY ((pk1_col, pk2_col)))");
        }

        onCassandra("CREATE INDEX ON tpch." + table + " (idx1_col)");
        onCassandra("CREATE INDEX ON tpch." + table + " (idx2_col)");

        onCassandra("INSERT INTO tpch." + table + "(pk1_col, pk2_col, cluster_col, idx1_col, idx2_col) VALUES('v11', 'v21', 1, 'value1', 'value21')");
        onCassandra("INSERT INTO tpch." + table + "(pk1_col, pk2_col, cluster_col, idx1_col, idx2_col) VALUES('v11', 'v22', 2, 'value1', 'value22')");
        onCassandra("INSERT INTO tpch." + table + "(pk1_col, pk2_col, cluster_col, idx1_col, idx2_col) VALUES('v12', 'v23', 1, 'value2', 'value23')");

        assertQuery("SELECT * FROM tpch." + table, "VALUES ('v11', 'v21', 1, 'value1', 'value21'), ('v11', 'v22', 2, 'value1', 'value22'), ('v12', 'v23', 1, 'value2', 'value23')");

        assertEventually(() -> {
            // Wait for indexes to be created successfully.
            assertThat(query("SELECT * FROM tpch." + table + " WHERE idx2_col = 'value1'")).isFullyPushedDown(); // Verify single indexed column predicate is pushed down.
        });

        // Execute a query on Trino with where clause having all the secondary key columns and another column (potentially a cluster key column)
        assertQuery("SELECT * FROM tpch." + table + " WHERE idx1_col = 'value1' AND idx2_col = 'value21' AND cluster_col = 1", "VALUES ('v11', 'v21', 1, 'value1', 'value21')");
        // Execute a query on Trino with where clause not having all the secondary key columns and another column (potentially a cluster key column)
        assertQuery("SELECT * FROM tpch." + table + " WHERE idx1_col = 'value1' AND cluster_col = 1", "VALUES ('v11', 'v21', 1, 'value1', 'value21')");

        onCassandra("DROP INDEX tpch." + table + "_idx1_col_idx");
        onCassandra("DROP INDEX tpch." + table + "_idx2_col_idx");

        onCassandra("DROP TABLE tpch." + table);
    }

    @Test
    public void testUpperCaseNameUnescapedInCassandra()
    {
        /*
         * If an identifier is not escaped with double quotes it is stored as lowercase in the Cassandra metadata
         *
         * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/ucase-lcase_r.html
         */
        session.execute("CREATE KEYSPACE KEYSPACE_1 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> computeActual("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_1")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE KEYSPACE_1.TABLE_1 (COLUMN_1 bigint PRIMARY KEY)");
        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.keyspace_1"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_1")
                .build(), new Duration(1, MINUTES));
        assertContains(computeActual("SHOW COLUMNS FROM cassandra.keyspace_1.table_1"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("column_1", "bigint", "", "")
                .build());

        assertUpdate("INSERT INTO keyspace_1.table_1 (column_1) VALUES (1)", 1);

        assertThat(computeActual("SELECT column_1 FROM cassandra.keyspace_1.table_1").getRowCount()).isEqualTo(1);
        assertUpdate("DROP TABLE cassandra.keyspace_1.table_1");

        // when an identifier is unquoted the lowercase and uppercase spelling may be used interchangeable
        session.execute("DROP KEYSPACE keyspace_1");
    }

    @Test
    public void testUppercaseNameEscaped()
    {
        /*
         * If an identifier is escaped with double quotes it is stored verbatim
         *
         * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/ucase-lcase_r.html
         */
        session.execute("CREATE KEYSPACE \"KEYSPACE_2\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> computeActual("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_2")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\" bigint PRIMARY KEY)");
        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.keyspace_2"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_2")
                .build(), new Duration(1, MINUTES));
        assertContains(computeActual("SHOW COLUMNS FROM cassandra.keyspace_2.table_2"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("column_2", "bigint", "", "")
                .build());

        assertUpdate("INSERT INTO \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\") VALUES (1)", 1);

        assertThat(computeActual("SELECT column_2 FROM cassandra.keyspace_2.table_2").getRowCount()).isEqualTo(1);
        assertUpdate("DROP TABLE cassandra.keyspace_2.table_2");

        // when an identifier is unquoted the lowercase and uppercase spelling may be used interchangeable
        session.execute("DROP KEYSPACE \"KEYSPACE_2\"");
    }

    @Test
    public void testKeyspaceNameAmbiguity()
    {
        // This test creates keyspaces that collide in a way not supported by the connector. Run it exclusively to prevent other tests from failing.
        executeExclusively(() -> {
            // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 keyspaces with names
            // that have differences only in letters case.
            session.execute("CREATE KEYSPACE \"KeYsPaCe_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE KEYSPACE \"kEySpAcE_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");

            // Although in Trino all the schema and table names are always displayed as lowercase
            assertContainsEventually(() -> computeActual("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("keyspace_3")
                    .row("keyspace_3")
                    .build(), new Duration(1, MINUTES));

            // There is no way to figure out what the exactly keyspace we want to retrieve tables from
            assertQueryFailsEventually(
                    "SHOW TABLES FROM cassandra.keyspace_3",
                    "Error listing tables for catalog cassandra: More than one keyspace has been found for the case insensitive schema name: keyspace_3 -> \\(KeYsPaCe_3, kEySpAcE_3\\)",
                    new Duration(1, MINUTES));

            session.execute("DROP KEYSPACE \"KeYsPaCe_3\"");
            session.execute("DROP KEYSPACE \"kEySpAcE_3\"");
            // Wait until the schema becomes invisible to Trino. Otherwise, testSelectInformationSchemaColumns may fail due to ambiguous schema names.
            assertEventually(() -> assertThat(computeActual("SHOW SCHEMAS FROM cassandra").getOnlyColumnAsSet())
                    .doesNotContain("keyspace_3"));
        });
    }

    @Test
    public void testTableNameAmbiguity()
    {
        // This test creates tables with names that collide in a way not supported by the connector. Run it exclusively to prevent other tests from failing.
        executeExclusively(() -> {
            session.execute("CREATE KEYSPACE keyspace_4 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
            assertContainsEventually(() -> computeActual("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("keyspace_4")
                    .build(), new Duration(1, MINUTES));

            // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 tables with names
            // that have differences only in letters case.
            session.execute("CREATE TABLE keyspace_4.\"TaBlE_4\" (column_4 bigint PRIMARY KEY)");
            session.execute("CREATE TABLE keyspace_4.\"tAbLe_4\" (column_4 bigint PRIMARY KEY)");

            // Although in Trino all the schema and table names are always displayed as lowercase
            assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.keyspace_4"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("table_4")
                    .row("table_4")
                    .build(), new Duration(1, MINUTES));

            // There is no way to figure out what the exactly table is being queried
            assertQueryFailsEventually(
                    "SHOW COLUMNS FROM cassandra.keyspace_4.table_4",
                    "More than one table has been found for the case insensitive table name: table_4 -> \\(TaBlE_4, tAbLe_4\\)",
                    new Duration(1, MINUTES));
            assertQueryFailsEventually(
                    "SELECT * FROM cassandra.keyspace_4.table_4",
                    "More than one table has been found for the case insensitive table name: table_4 -> \\(TaBlE_4, tAbLe_4\\)",
                    new Duration(1, MINUTES));
            session.execute("DROP KEYSPACE keyspace_4");
        });
    }

    @Test
    public void testColumnNameAmbiguity()
    {
        // This test creates columns with names that collide in a way not supported by the connector. Run it exclusively to prevent other tests from failing.
        executeExclusively(() -> {
            session.execute("CREATE KEYSPACE keyspace_5 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
            assertContainsEventually(() -> computeActual("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("keyspace_5")
                    .build(), new Duration(1, MINUTES));

            session.execute("CREATE TABLE keyspace_5.table_5 (\"CoLuMn_5\" bigint PRIMARY KEY, \"cOlUmN_5\" bigint)");
            assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.keyspace_5"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("table_5")
                    .build(), new Duration(1, MINUTES));

            assertQueryFailsEventually(
                    "SHOW COLUMNS FROM cassandra.keyspace_5.table_5",
                    "More than one column has been found for the case insensitive column name: column_5 -> \\(CoLuMn_5, cOlUmN_5\\)",
                    new Duration(1, MINUTES));
            assertQueryFailsEventually(
                    "SELECT * FROM cassandra.keyspace_5.table_5",
                    "More than one column has been found for the case insensitive column name: column_5 -> \\(CoLuMn_5, cOlUmN_5\\)",
                    new Duration(1, MINUTES));

            session.execute("DROP KEYSPACE keyspace_5");
        });
    }

    @Test
    public void testUserDefinedTypeInArray()
    {
        String tableName = "test_udt_in_array" + randomNameSuffix();
        String userDefinedTypeName = "test_udt" + randomNameSuffix();

        session.execute("CREATE TYPE tpch." + userDefinedTypeName + "(udt_field bigint)");
        session.execute("CREATE TABLE tpch." + tableName + "(id bigint, col list<frozen<tpch." + userDefinedTypeName + ">>, primary key (id))");
        session.execute("INSERT INTO tpch." + tableName + "(id, col) values (1, [{udt_field: 10}])");
        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), VARCHAR)
                .row(tableName)
                .build(), new Duration(1, MINUTES));

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, '[\"{udt_field:10}\"]')");

        session.execute("DROP TABLE tpch." + tableName);
        session.execute("DROP TYPE tpch." + userDefinedTypeName);
    }

    @Test
    public void testUserDefinedTypeInMap()
    {
        String tableName = "test_udt_in_map" + randomNameSuffix();
        String userDefinedTypeName = "test_udt" + randomNameSuffix();

        session.execute("CREATE TYPE tpch." + userDefinedTypeName + "(udt_field bigint)");
        session.execute("CREATE TABLE tpch." + tableName + "(id bigint, col map<frozen<tpch." + userDefinedTypeName + ">, frozen<tpch." + userDefinedTypeName + ">>, primary key (id))");
        session.execute("INSERT INTO tpch." + tableName + "(id, col) values (1, {{udt_field: 10}: {udt_field: -10}})");
        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), VARCHAR)
                .row(tableName)
                .build(), new Duration(1, MINUTES));

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, '{\"{udt_field:10}\":\"{udt_field:-10}\"}')");

        session.execute("DROP TABLE tpch." + tableName);
        session.execute("DROP TYPE tpch." + userDefinedTypeName);
    }

    @Test
    public void testUserDefinedTypeInSet()
    {
        String tableName = "test_udt_in_set" + randomNameSuffix();
        String userDefinedTypeName = "test_udt" + randomNameSuffix();

        session.execute("CREATE TYPE tpch." + userDefinedTypeName + "(udt_field bigint)");
        session.execute("CREATE TABLE tpch." + tableName + "(id bigint, col set<frozen<tpch." + userDefinedTypeName + ">>, primary key (id))");
        session.execute("INSERT INTO tpch." + tableName + "(id, col) values (1, {{udt_field: 10}})");
        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), VARCHAR)
                .row(tableName)
                .build(), new Duration(1, MINUTES));

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, '[\"{udt_field:10}\"]')");

        session.execute("DROP TABLE tpch." + tableName);
        session.execute("DROP TYPE tpch." + userDefinedTypeName);
    }

    @Test
    void testPartitioningKeys()
    {
        try (TestCassandraTable table = testTable(
                "test_partitioning_keys",
                ImmutableList.of(generalColumn("data", "int"), partitionColumn("part", "int")),
                ImmutableList.of("1, 10", "2, 20"))) {
            assertThat(query("SELECT part FROM " + table.getTableName() + " WHERE part = 10"))
                    .matches("VALUES 10");
            assertThat(query("SELECT part FROM " + table.getTableName() + " WHERE data = 1"))
                    .matches("VALUES 10");
        }
    }

    @Test
    void testSelectClusteringMaterializedView()
    {
        try (TestCassandraTable table = testTable(
                "test_clustering_materialized_view_base",
                ImmutableList.of(generalColumn("id", "int"), generalColumn("data", "int"), partitionColumn("key", "int")),
                ImmutableList.of("1, 10, 100", "2, 20, 200", "3, 30, 300"))) {
            String mvName = "test_clustering_mv" + randomNameSuffix();
            onCassandra("CREATE MATERIALIZED VIEW tpch." + mvName + " AS " +
                    "SELECT * FROM " + table.getTableName() + " WHERE id IS NOT NULL " +
                    "PRIMARY KEY (id, key) " +
                    "WITH CLUSTERING ORDER BY (id DESC)");

            assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), VARCHAR)
                    .row(mvName)
                    .build(), new Duration(1, MINUTES));

            // Materialized view may not return all results during the creation
            assertContainsEventually(() -> computeActual("SELECT count(*) FROM tpch." + mvName), resultBuilder(getSession(), BIGINT)
                    .row(3L)
                    .build(), new Duration(1, MINUTES));

            assertThat(query("SELECT MAX(id), SUM(key), AVG(data) FROM " + table.getTableName() + " WHERE key BETWEEN 100 AND 200"))
                    .matches("VALUES (2, BIGINT '300', DOUBLE '15.0')");

            assertThat(query("SELECT id, key, data FROM " + table.getTableName() + " ORDER BY id LIMIT 1"))
                    .matches("VALUES (1, 100, 10)");

            onCassandra("DROP MATERIALIZED VIEW tpch." + mvName);
        }
    }

    @Test
    void testSelectTupleTypeInPrimaryKey()
    {
        try (TestCassandraTable table = testTable(
                "test_tuple_in_primary_key",
                ImmutableList.of(partitionColumn("intkey", "int"), partitionColumn("tuplekey", "frozen<tuple<int, text, float>>")),
                ImmutableList.of("1, (1, 'text-1', 1.11)"))) {
            assertThat(query("SELECT * FROM " + table.getTableName()))
                    .matches("VALUES (1, CAST(ROW(1, 'text-1', 1.11) AS ROW(integer, varchar, real)))");
            assertThat(query("SELECT * FROM " + table.getTableName() + " WHERE intkey = 1 AND tuplekey = row(1, 'text-1', 1.11)"))
                    .matches("VALUES (1, CAST(ROW(1, 'text-1', 1.11) AS ROW(integer, varchar, real)))");
        }
    }

    @Test
    void testSelectUserDefinedTypeInPrimaryKey()
    {
        String udtName = "type_user_defined_primary_key" + randomNameSuffix();
        onCassandra("CREATE TYPE tpch." + udtName + " (field1 text)");
        try (TestCassandraTable table = testTable(
                "test_udt_in_primary_key",
                ImmutableList.of(partitionColumn("intkey", "int"), partitionColumn("udtkey", "frozen<%s>".formatted(udtName))),
                ImmutableList.of("1, {field1: 'udt-1'}"))) {
            assertThat(query("SELECT * FROM " + table.getTableName()))
                    .matches("VALUES (1, CAST(ROW('udt-1') AS ROW(field1 VARCHAR)))");
            assertThat(query("SELECT * FROM " + table.getTableName() + " WHERE intkey = 1 AND udtkey = CAST(ROW('udt-1') AS ROW(x VARCHAR))"))
                    .matches("VALUES (1, CAST(ROW('udt-1') AS ROW(field1 VARCHAR)))");
        }
        onCassandra("DROP TYPE tpch." + udtName);
    }

    @Test
    public void testUnsupportedColumnType()
    {
        // TODO currently all standard types are supported to some extent. We should add a test with custom type if possible.
    }

    @Test
    public void testNullAndEmptyTimestamp()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "test_empty_timestamp",
                ImmutableList.of(
                        partitionColumn("id", "int"),
                        generalColumn("timestamp_column_with_null", "timestamp"),
                        generalColumn("timestamp_column_with_empty", "timestamp")),
                ImmutableList.of("1, NULL, ''"))) {
            String tableName = testCassandraTable.getTableName();

            assertThat(query(format("SELECT timestamp_column_with_null FROM %s", tableName)))
                    .matches("VALUES CAST(NULL AS timestamp(3) with time zone)");
            assertThat(query(format("SELECT timestamp_column_with_empty FROM %s", tableName)))
                    .matches("VALUES CAST(NULL AS timestamp(3) with time zone)");

            assertThat(query(format("SELECT id FROM %s WHERE timestamp_column_with_null IS NULL", tableName)))
                    .matches("VALUES 1");
            assertThat(query(format("SELECT id FROM %s WHERE timestamp_column_with_empty IS NULL", tableName)))
                    .matches("VALUES 1");
        }
    }

    @Test
    public void testEmptyTimestampClusteringKey()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "test_empty_timestamp",
                ImmutableList.of(
                        partitionColumn("id", "int"),
                        partitionColumn("timestamp_column_with_empty", "timestamp")),
                ImmutableList.of("1, ''"))) {
            String tableName = testCassandraTable.getTableName();

            assertThat(query(format("SELECT timestamp_column_with_empty FROM %s", tableName)))
                    .matches("VALUES CAST(NULL AS timestamp(3) with time zone)");

            assertThat(query(format("SELECT id FROM %s WHERE timestamp_column_with_empty IS NULL", tableName)))
                    .matches("VALUES 1");
        }
    }

    @Test
    public void testNestedCollectionType()
    {
        session.execute("CREATE KEYSPACE keyspace_test_nested_collection WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> computeActual("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_test_nested_collection")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE keyspace_test_nested_collection.table_set (column_5 bigint PRIMARY KEY, nested_collection frozen<set<set<bigint>>>)");
        session.execute("CREATE TABLE keyspace_test_nested_collection.table_list (column_5 bigint PRIMARY KEY, nested_collection frozen<list<list<bigint>>>)");
        session.execute("CREATE TABLE keyspace_test_nested_collection.table_map (column_5 bigint PRIMARY KEY, nested_collection frozen<map<int, map<bigint, bigint>>>)");

        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.keyspace_test_nested_collection"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_set")
                .row("table_list")
                .row("table_map")
                .build(), new Duration(1, MINUTES));

        session.execute("INSERT INTO keyspace_test_nested_collection.table_set (column_5, nested_collection) VALUES (1, {{1, 2, 3}})");
        assertThat(computeActual("SELECT nested_collection FROM cassandra.keyspace_test_nested_collection.table_set").getMaterializedRows().get(0)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, "[[1,2,3]]"));

        session.execute("INSERT INTO keyspace_test_nested_collection.table_list (column_5, nested_collection) VALUES (1, [[4, 5, 6]])");
        assertThat(computeActual("SELECT nested_collection FROM cassandra.keyspace_test_nested_collection.table_list").getMaterializedRows().get(0)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, "[[4,5,6]]"));

        session.execute("INSERT INTO keyspace_test_nested_collection.table_map (column_5, nested_collection) VALUES (1, {7:{8:9}})");
        assertThat(computeActual("SELECT nested_collection FROM cassandra.keyspace_test_nested_collection.table_map").getMaterializedRows().get(0)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, "{7:{8:9}}"));

        session.execute("DROP KEYSPACE keyspace_test_nested_collection");
    }

    @Test
    public void testAllTypesInsert()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_all_types_insert",
                ImmutableList.of(
                        partitionColumn("key", "text"),
                        generalColumn("typeuuid", "uuid"),
                        generalColumn("typetinyint", "tinyint"),
                        generalColumn("typesmallint", "smallint"),
                        generalColumn("typeinteger", "int"),
                        generalColumn("typelong", "bigint"),
                        generalColumn("typebytes", "blob"),
                        generalColumn("typedate", "date"),
                        generalColumn("typetimestamp", "timestamp"),
                        generalColumn("typeansi", "ascii"),
                        generalColumn("typeboolean", "boolean"),
                        generalColumn("typedecimal", "decimal"),
                        generalColumn("typedouble", "double"),
                        generalColumn("typefloat", "float"),
                        generalColumn("typeinet", "inet"),
                        generalColumn("typevarchar", "varchar"),
                        generalColumn("typevarint", "varint"),
                        generalColumn("typetimeuuid", "timeuuid"),
                        generalColumn("typelist", "frozen <list<text>>"),
                        generalColumn("typemap", "frozen <map<int, bigint>>"),
                        generalColumn("typeset", "frozen <set<boolean>>")),
                ImmutableList.of())) {
            String sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                    "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                    " FROM " + testCassandraTable.getTableName();
            assertThat(computeActual(sql).getRowCount()).isEqualTo(0);

            // TODO Following types are not supported now. We need to change null into the value after fixing it
            // blob, frozen<set<type>>, list<type>, map<type,type>, set<type>, decimal, varint
            // timestamp can be inserted but the expected and actual values are not same
            assertUpdate("INSERT INTO " + testCassandraTable.getTableName() + " (" +
                    "key," +
                    "typeuuid," +
                    "typeinteger," +
                    "typelong," +
                    "typebytes," +
                    "typetimestamp," +
                    "typeansi," +
                    "typeboolean," +
                    "typedecimal," +
                    "typedouble," +
                    "typefloat," +
                    "typeinet," +
                    "typevarchar," +
                    "typevarint," +
                    "typetimeuuid," +
                    "typelist," +
                    "typemap," +
                    "typeset" +
                    ") VALUES (" +
                    "'key1', " +
                    "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59', " +
                    "1, " +
                    "1000, " +
                    "null, " +
                    "timestamp '1970-01-01 08:34:05.0Z', " +
                    "'ansi1', " +
                    "true, " +
                    "null, " +
                    "0.3, " +
                    "cast('0.4' as real), " +
                    "IPADDRESS '10.10.10.1', " +
                    "'varchar1', " +
                    "null, " +
                    "UUID '50554d6e-29bb-11e5-b345-feff819cdc9f', " +
                    "null, " +
                    "null, " +
                    "null " +
                    ")",
                    1);

            MaterializedResult result = computeActual(sql);
            int rowCount = result.getRowCount();
            assertThat(rowCount).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                    "key1",
                    java.util.UUID.fromString("12151fd2-7586-11e9-8f9e-2a86e4085a59"),
                    1,
                    1000L,
                    null,
                    ZonedDateTime.of(1970, 1, 1, 8, 34, 5, 0, ZoneId.of("UTC")),
                    "ansi1",
                    true,
                    null,
                    0.3,
                    (float) 0.4,
                    "10.10.10.1",
                    "varchar1",
                    null,
                    java.util.UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"),
                    null,
                    null,
                    null));

            // insert null for all datatypes
            assertUpdate("INSERT INTO " + testCassandraTable.getTableName() + " (" +
                    "key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal," +
                    "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                    ") VALUES (" +
                    "'key2', null, null, null, null, null, null, null, null," +
                    "null, null, null, null, null, null, null, null, null)",
                    1);
            sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                    "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                    " FROM " + testCassandraTable.getTableName() + " WHERE key = 'key2'";
            result = computeActual(sql);
            rowCount = result.getRowCount();
            assertThat(rowCount).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                    "key2", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));

            // insert into only a subset of columns
            assertUpdate("INSERT INTO " + testCassandraTable.getTableName() + " (" +
                    "key, typeinteger, typeansi, typeboolean) VALUES (" +
                    "'key3', 999, 'ansi', false)", 1);
            sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                    "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                    " FROM " + testCassandraTable.getTableName() + " WHERE key = 'key3'";
            result = computeActual(sql);
            rowCount = result.getRowCount();
            assertThat(rowCount).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                    "key3", null, 999, null, null, null, "ansi", false, null, null, null, null, null, null, null, null, null, null));
        }
    }

    @Test
    @Override
    public void testDelete()
    {
        try (TestCassandraTable testCassandraTable = testTable(
                "table_delete_data",
                ImmutableList.of(
                        partitionColumn("partition_one", "bigint"),
                        partitionColumn("partition_two", "int"),
                        clusterColumn("clust_one", "text"),
                        generalColumn("data", "text")),
                ImmutableList.of(
                        "1, 1, 'clust_one_1', null",
                        "2, 2, 'clust_one_2', null",
                        "3, 3, 'clust_one_3', null",
                        "4, 4, 'clust_one_4', null",
                        "5, 5, 'clust_one_5', null",
                        "6, 6, 'clust_one_6', null",
                        "7, 7, 'clust_one_7', null",
                        "8, 8, 'clust_one_8', null",
                        "9, 9, 'clust_one_9', null",
                        "1, 1, 'clust_one_2', null",
                        "1, 1, 'clust_one_3', null",
                        "1, 2, 'clust_one_1', null",
                        "1, 2, 'clust_one_2', null",
                        "1, 2, 'clust_one_3', null",
                        "2, 2, 'clust_one_1', null"))) {
            String keyspaceAndTable = testCassandraTable.getTableName();
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable).getRowCount()).isEqualTo(15);

            // error
            assertThat(query("DELETE FROM " + keyspaceAndTable))
                    .failure().hasMessage("Deleting without partition key is not supported");
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable).getRowCount()).isEqualTo(15);

            String whereClusteringKeyOnly = " WHERE clust_one='clust_one_2'";
            assertThat(query("DELETE FROM " + keyspaceAndTable + whereClusteringKeyOnly))
                    .failure().hasMessage("Delete without primary key or partition key is not supported");
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable).getRowCount()).isEqualTo(15);

            String whereMultiplePartitionKeyWithClusteringKey = " WHERE " +
                    " (partition_one=1 AND partition_two=1 AND clust_one='clust_one_1') OR " +
                    " (partition_one=1 AND partition_two=2 AND clust_one='clust_one_2') ";
            assertThat(query("DELETE FROM " + keyspaceAndTable + whereMultiplePartitionKeyWithClusteringKey))
                    .failure().hasMessage("Delete without primary key or partition key is not supported");
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable).getRowCount()).isEqualTo(15);

            // success
            String wherePrimaryKey = " WHERE partition_one=3 AND partition_two=3 AND clust_one='clust_one_3'";
            assertUpdate("DELETE FROM " + keyspaceAndTable + wherePrimaryKey);
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable).getRowCount()).isEqualTo(14);
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable + wherePrimaryKey).getRowCount()).isEqualTo(0);

            String wherePartitionKey = " WHERE partition_one=2 AND partition_two=2";
            assertUpdate("DELETE FROM " + keyspaceAndTable + wherePartitionKey);
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable).getRowCount()).isEqualTo(12);
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable + wherePartitionKey).getRowCount()).isEqualTo(0);

            String whereMultiplePartitionKey = " WHERE (partition_one=1 AND partition_two=1) OR (partition_one=1 AND partition_two=3)";
            assertUpdate("DELETE FROM " + keyspaceAndTable + whereMultiplePartitionKey);
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable).getRowCount()).isEqualTo(9);
            assertThat(computeActual("SELECT * FROM " + keyspaceAndTable + whereMultiplePartitionKey).getRowCount()).isEqualTo(0);
        }
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Test
    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Test
    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Test
    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Test
    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Test
    @Override
    public void testDeleteWithVarcharPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        assertThatThrownBy(super::testDeleteAllDataFromTable)
                .hasStackTraceContaining("Deleting without partition key is not supported");
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    // test polymorphic table function

    @Test
    public void testNativeQuerySelectFromNation()
    {
        assertQuery(
                "SELECT * FROM TABLE(cassandra.system.query(query => 'SELECT name FROM tpch.nation WHERE nationkey = 0 ALLOW FILTERING'))",
                "VALUES 'ALGERIA'");
        assertQuery(
                "SELECT name FROM TABLE(cassandra.system.query(query => 'SELECT * FROM tpch.nation WHERE nationkey = 0 ALLOW FILTERING'))",
                "VALUES 'ALGERIA'");
        assertQuery(
                "SELECT name FROM TABLE(cassandra.system.query(query => 'SELECT * FROM tpch.nation')) WHERE nationkey = 0",
                "VALUES 'ALGERIA'");
        assertThat(query("SELECT * FROM TABLE(cassandra.system.query(query => 'SELECT * FROM tpch.nation')) WHERE nationkey = 0"))
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testNativeQueryColumnAlias()
    {
        assertThat(query("SELECT region_name FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region WHERE regionkey = 0 ALLOW FILTERING'))"))
                .matches("VALUES CAST('AFRICA' AS VARCHAR)");
    }

    @Test
    public void testNativeQueryColumnAliasNotFound()
    {
        assertQueryFails(
                "SELECT name FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region'))",
                ".* Column 'name' cannot be resolved");
        assertQueryFails(
                "SELECT column_not_found FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region'))",
                ".* Column 'column_not_found' cannot be resolved");
    }

    @Test
    public void testNativeQuerySelectFromTestTable()
    {
        String tableName = "test_select" + randomNameSuffix();
        onCassandra("CREATE TABLE tpch." + tableName + "(col BIGINT PRIMARY KEY)");
        onCassandra("INSERT INTO tpch." + tableName + "(col) VALUES (1)");
        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row(tableName)
                .build(), new Duration(1, MINUTES));

        assertQuery(
                "SELECT * FROM TABLE(cassandra.system.query(query => 'SELECT * FROM tpch." + tableName + "'))",
                "VALUES 1");

        onCassandra("DROP TABLE tpch." + tableName);
    }

    @Test
    public void testNativeQueryCaseSensitivity()
    {
        // This test creates columns with names that collide in a way not supported by the connector. Run it exclusively to prevent other tests from failing.
        executeExclusively(() -> {
            String tableName = "test_case" + randomNameSuffix();
            onCassandra("CREATE TABLE tpch." + tableName + "(col_case BIGINT PRIMARY KEY, \"COL_CASE\" BIGINT)");
            onCassandra("INSERT INTO tpch." + tableName + "(col_case, \"COL_CASE\") VALUES (1, 2)");
            assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row(tableName)
                    .build(), new Duration(1, MINUTES));

            assertQuery(
                    "SELECT * FROM TABLE(cassandra.system.query(query => 'SELECT * FROM tpch." + tableName + "'))",
                    "VALUES (1, 2)");

            onCassandra("DROP TABLE tpch." + tableName);
            // Wait until the table becomes invisible to Trino. Otherwise, testSelectInformationSchemaColumns may fail due to ambiguous column names.
            assertEventually(() -> assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse());
        });
    }

    @Test
    public void testNativeQueryCreateTableFailure()
    {
        String tableName = "test_create" + randomNameSuffix();
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(query("SELECT * FROM TABLE(cassandra.system.query(query => 'CREATE TABLE tpch." + tableName + "(col INT PRIMARY KEY)'))"))
                .failure().hasMessage("Cannot get column definition");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testNativeQueryPreparingStatementFailure()
    {
        String tableName = "test_insert" + randomNameSuffix();
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(query("SELECT * FROM TABLE(cassandra.system.query(query => 'INSERT INTO tpch." + tableName + "(col) VALUES (1)'))"))
                .failure()
                .hasMessage("Cannot get column definition")
                .hasStackTraceContaining("unconfigured table");
    }

    @Test
    public void testNativeQueryUnsupportedStatement()
    {
        String tableName = "test_unsupported_statement" + randomNameSuffix();
        onCassandra("CREATE TABLE tpch." + tableName + "(col INT PRIMARY KEY)");
        onCassandra("INSERT INTO tpch." + tableName + "(col) VALUES (1)");
        assertContainsEventually(() -> computeActual("SHOW TABLES FROM cassandra.tpch"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row(tableName)
                .build(), new Duration(1, MINUTES));

        assertThat(query("SELECT * FROM TABLE(cassandra.system.query(query => 'INSERT INTO tpch." + tableName + "(col) VALUES (3)'))"))
                .failure().hasMessage("Cannot get column definition");
        assertThat(query("SELECT * FROM TABLE(cassandra.system.query(query => 'DELETE FROM tpch." + tableName + " WHERE col = 1'))"))
                .failure().hasMessage("Cannot get column definition");

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        onCassandra("DROP TABLE IF EXISTS tpch." + tableName);
    }

    @Test
    public void testNativeQueryIncorrectSyntax()
    {
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .failure()
                .hasMessage("Cannot get column definition")
                .hasStackTraceContaining("no viable alternative at input 'some'");
    }

    @Test
    void testExecuteProcedure()
    {
        try (TestCassandraTable table = testTable(
                "execute_procedure",
                ImmutableList.of(partitionColumn("key", "int")),
                ImmutableList.of())) {
            String keyspaceAndTable = table.getTableName();
            assertUpdate("CALL system.execute('INSERT INTO " + keyspaceAndTable + " (key) VALUES (1)')");
            assertQuery("SELECT * FROM " + keyspaceAndTable, "VALUES 1");

            assertUpdate("CALL system.execute('DELETE FROM " + keyspaceAndTable + " WHERE key=1')");
            assertQueryReturnsEmptyResult("SELECT * FROM " + keyspaceAndTable);
        }
    }

    @Test
    void testExecuteProcedureWithNamedArgument()
    {
        String tableName = "execute_procedure" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;

        assertUpdate("CREATE TABLE " + schemaTableName + "(a int)");
        try {
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertUpdate("CALL system.execute(query => 'DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    void testExecuteProcedureWithInvalidQuery()
    {
        assertQueryFails("CALL system.execute('SELECT 1')", "(?s).*no viable alternative at input.*");
        assertQueryFails("CALL system.execute('invalid')", "(?s).*no viable alternative at input.*");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(65535);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Attempted serializing to buffer exceeded maximum of 65535 bytes:");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(48);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Table names shouldn't be more than 48 characters long");
    }

    @Test
    public void testNationJoinNation()
    {
        assertQuery("SELECT n1.name, n2.regionkey " +
                        "FROM nation n1 JOIN nation n2 ON n1.nationkey = n2.regionkey " +
                        "WHERE n1.nationkey = 3",
                "VALUES ('CANADA', 3), ('CANADA', 3), ('CANADA', 3), ('CANADA', 3), ('CANADA', 3)");
    }

    @Test
    public void testNationJoinRegion()
    {
        assertQuery("SELECT c.name, t.name " +
                        "FROM nation c JOIN tpch.tiny.region t ON c.regionkey = t.regionkey " +
                        "WHERE c.nationkey = 3",
                "VALUES ('CANADA', 'AMERICA')");
    }

    @Test
    public void testProtocolVersion()
    {
        assertQuery("SELECT native_protocol_version FROM system.local",
                "VALUES 4");
    }

    private void assertSelect(String tableName)
    {
        String sql = "SELECT " +
                " key, " +
                " typeuuid, " +
                " typeinteger, " +
                " typelong, " +
                " typebytes, " +
                " typetimestamp, " +
                " typeansi, " +
                " typeboolean, " +
                " typedecimal, " +
                " typedouble, " +
                " typefloat, " +
                " typeinet, " +
                " typevarchar, " +
                " typevarint, " +
                " typetimeuuid, " +
                " typelist, " +
                " typemap, " +
                " typeset " +
                " FROM " + tableName;

        MaterializedResult result = computeActual(sql);

        int rowCount = result.getRowCount();
        assertThat(rowCount).isEqualTo(9);
        assertThat(result.getTypes()).isEqualTo(ImmutableList.of(
                createUnboundedVarcharType(),
                UUID,
                INTEGER,
                BIGINT,
                VARBINARY,
                TIMESTAMP_TZ_MILLIS,
                createUnboundedVarcharType(),
                BOOLEAN,
                DOUBLE,
                DOUBLE,
                REAL,
                IPADDRESS,
                createUnboundedVarcharType(),
                createUnboundedVarcharType(),
                UUID,
                createUnboundedVarcharType(),
                createUnboundedVarcharType(),
                createUnboundedVarcharType()));

        List<MaterializedRow> sortedRows = result.getMaterializedRows().stream()
                .sorted(comparing(o -> o.getField(1).toString()))
                .collect(toList());

        for (int rowNumber = 1; rowNumber <= rowCount; rowNumber++) {
            assertThat(sortedRows.get(rowNumber - 1)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                    "key " + rowNumber,
                    java.util.UUID.fromString(format("00000000-0000-0000-0000-%012d", rowNumber)),
                    rowNumber,
                    rowNumber + 1000L,
                    Bytes.fromBytes(Ints.toByteArray(rowNumber)),
                    TIMESTAMP_VALUE,
                    "ansi " + rowNumber,
                    rowNumber % 2 == 0,
                    Math.pow(2, rowNumber),
                    Math.pow(4, rowNumber),
                    (float) Math.pow(8, rowNumber),
                    "127.0.0.1",
                    "varchar " + rowNumber,
                    BigInteger.TEN.pow(rowNumber).toString(),
                    java.util.UUID.fromString(format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber)),
                    format("[\"list-value-1%1$d\",\"list-value-2%1$d\"]", rowNumber),
                    format("{%d:%d,%d:%d}", rowNumber, rowNumber + 1L, rowNumber + 2, rowNumber + 3L),
                    "[false,true]"));
        }
    }

    private TestCassandraTable testTable(String namePrefix, List<TestCassandraTable.ColumnDefinition> columnDefinitions, List<String> rowsToInsert)
    {
        String keyspace = getQueryRunner().getDefaultSession().getSchema().orElseThrow();
        return new TestCassandraTable(getQueryRunner(), session::execute, keyspace, namePrefix, columnDefinitions, rowsToInsert);
    }

    private void onCassandra(@Language("SQL") String sql)
    {
        session.execute(sql);
    }
}
