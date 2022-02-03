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
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.Bytes;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.assertions.Assert;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.datastax.driver.core.utils.Bytes.toRawHexString;
import static io.trino.plugin.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static io.trino.plugin.cassandra.CassandraQueryRunner.createCassandraSession;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES_INSERT;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES_PARTITION_KEY;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS_INEQUALITY;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS_LARGE;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_DELETE_DATA;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_MULTI_PARTITION_CLUSTERING_KEYS;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_PUSHDOWN_UUID_PARTITION_KEY_PREDICATE;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createTestTables;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.QueryAssertions.assertContainsEventually;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestCassandraConnectorTest
        extends BaseConnectorTest
{
    private static final String KEYSPACE = "smoke_test";
    private static final Session SESSION = createCassandraSession(KEYSPACE);

    private static final ZonedDateTime TIMESTAMP_VALUE = ZonedDateTime.of(1970, 1, 1, 3, 4, 5, 0, ZoneId.of("UTC"));

    private CassandraServer server;
    private CassandraSession session;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return false;

            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_DELETE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new CassandraServer());
        session = server.getSession();
        createTestTables(session, KEYSPACE, Timestamp.from(TIMESTAMP_VALUE.toInstant()));
        return createCassandraQueryRunner(server, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Cassandra connector does not support column default values");
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
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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

        Assert.assertEquals(actual, expectedParametrizedVarchar);
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        Assert.assertEquals(actualColumns, expectedColumns);
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

    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessage("unsupported type: char(3)");
    }

    @Test
    public void testPushdownUuidPartitionKeyPredicate()
    {
        assertThat(query(format("SELECT col_text FROM %s.%s WHERE col_uuid = UUID '00000000-0000-0000-0000-000000000001'", KEYSPACE, TABLE_PUSHDOWN_UUID_PARTITION_KEY_PREDICATE)))
                .matches("VALUES CAST('Trino' AS varchar)");
    }

    @Test
    public void testPartitionKeyPredicate()
    {
        String sql = "SELECT *" +
                " FROM " + TABLE_ALL_TYPES_PARTITION_KEY +
                " WHERE key = 'key 7'" +
                " AND typeuuid = UUID '00000000-0000-0000-0000-000000000007'" +
                " AND typetinyint = 7" +
                " AND typesmallint = 7" +
                " AND typeinteger = 7" +
                " AND typelong = 1007" +
                " AND typebytes = from_hex('" + toRawHexString(ByteBuffer.wrap(Ints.toByteArray(7))) + "')" +
                " AND typedate = DATE '1970-01-01'" +
                " AND typetimestamp = TIMESTAMP '1970-01-01 03:04:05Z'" +
                " AND typeansi = 'ansi 7'" +
                " AND typeboolean = false" +
                " AND typedecimal = 128.0" +
                " AND typedouble = 16384.0" +
                " AND typefloat = REAL '2097152.0'" +
                " AND typeinet = '127.0.0.1'" +
                " AND typevarchar = 'varchar 7'" +
                " AND typevarint = '10000000'" +
                " AND typetimeuuid = UUID 'd2177dd0-eaa2-11de-a572-001b779c76e7'" +
                " AND typelist = '[\"list-value-17\",\"list-value-27\"]'" +
                " AND typemap = '{7:8,9:10}'" +
                " AND typeset = '[false,true]'" +
                "";
        MaterializedResult result = execute(sql);

        assertEquals(result.getRowCount(), 1);
    }

    @Test
    public void testTimestampPartitionKey()
            throws Exception
    {
        String tableName = "test_timestamp_" + Math.abs(ThreadLocalRandom.current().nextLong());
        session.execute(format("CREATE TABLE %s.%s (c1 timestamp primary key)", KEYSPACE, tableName));
        session.execute(format("INSERT INTO %s.%s (c1) VALUES ('2017-04-01T11:21:59.001+0000')", KEYSPACE, tableName));
        server.refreshSizeEstimates(KEYSPACE, tableName);

        try {
            String sql = format(
                    "SELECT * " +
                            "FROM %s " +
                            "WHERE c1 = TIMESTAMP '2017-04-01 11:21:59.001 UTC'", tableName);
            MaterializedResult result = execute(sql);

            assertEquals(result.getRowCount(), 1);
        }
        finally {
            session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
        }
    }

    @Test
    public void testSelect()
    {
        assertSelect(TABLE_ALL_TYPES, false);
        assertSelect(TABLE_ALL_TYPES_PARTITION_KEY, false);
    }

    @Test
    public void testInsertToTableWithHiddenId()
    {
        execute("DROP TABLE IF EXISTS test_create_table");
        execute("CREATE TABLE test_create_table (col1 integer)");
        execute("INSERT INTO test_create_table VALUES (12345)");
        assertQuery("SELECT * FROM smoke_test.test_create_table", "VALUES (12345)");
        execute("DROP TABLE test_create_table");
    }

    @Test
    public void testCreateTableAs()
    {
        execute("DROP TABLE IF EXISTS table_all_types_copy");
        execute("CREATE TABLE table_all_types_copy AS SELECT * FROM " + TABLE_ALL_TYPES);
        assertSelect("table_all_types_copy", true);
        execute("DROP TABLE table_all_types_copy");
    }

    @Test
    public void testIdentifiers()
    {
        session.execute("DROP KEYSPACE IF EXISTS \"_keyspace\"");
        session.execute("CREATE KEYSPACE \"_keyspace\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("_keyspace")
                .build(), new Duration(1, MINUTES));

        execute("CREATE TABLE _keyspace._table AS SELECT 1 AS \"_col\", 2 AS \"2col\"");
        assertQuery("SHOW TABLES FROM cassandra._keyspace", "VALUES ('_table')");
        assertQuery("SELECT * FROM cassandra._keyspace._table", "VALUES (1, 2)");
        assertUpdate("DROP TABLE cassandra._keyspace._table");

        session.execute("DROP KEYSPACE \"_keyspace\"");
    }

    @Test
    public void testClusteringPredicates()
    {
        String sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key='key_1' AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key='key_1' AND clust_one!='clust_one'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2','key_3','key_4') AND clust_one='clust_one' AND clust_two>'clust_two_1'";
        assertEquals(execute(sql).getRowCount(), 3);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1') OR (clust_two='clust_two_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_three='clust_three_1'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')";
        assertEquals(execute(sql).getRowCount(), 2);
    }

    @Test
    public void testMultiplePartitionClusteringPredicates()
    {
        String partitionInPredicates = " partition_one IN ('partition_one_1','partition_one_2') AND partition_two IN ('partition_two_1','partition_two_2') ";
        String sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one!='clust_one'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " +
                "partition_one IN ('partition_one_1','partition_one_2','partition_one_3','partition_one_4') AND " +
                "partition_two IN ('partition_two_1','partition_two_2','partition_two_3','partition_two_4') AND " +
                "clust_one='clust_one' AND clust_two>'clust_two_1'";
        assertEquals(execute(sql).getRowCount(), 3);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1') OR (clust_two='clust_two_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND clust_three='clust_three_1'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')";
        assertEquals(execute(sql).getRowCount(), 2);
    }

    @Test
    public void testClusteringKeyOnlyPushdown()
    {
        String sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 9);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE clust_one='clust_one' AND clust_two='clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'";
        assertEquals(execute(sql).getRowCount(), 1);

        // below test cases are needed to verify clustering key pushdown with unpartitioned table
        // for the smaller table (<200 partitions by default) connector fetches all the partitions id
        // and the partitioned patch is being followed
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two='clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two > 'clust_two_998'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two > 'clust_two_997' AND clust_two < 'clust_two_999'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_998'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three < 'clust_three_3'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_1' AND clust_three < 'clust_three_3'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two < 'clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_997','clust_two_998','clust_two_999') AND clust_two > 'clust_two_998'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two = 'clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
    }

    @Test
    public void testNotEqualPredicateOnClusteringColumn()
    {
        String sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one != 'clust_one'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two != 2";
        assertEquals(execute(sql).getRowCount(), 3);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two >= 2 AND clust_two != 3";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two > 2 AND clust_two != 3";
        assertEquals(execute(sql).getRowCount(), 1);
    }

    @Test
    public void testClusteringKeyPushdownInequality()
    {
        String sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 4);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1970-01-01 03:04:05.020Z'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1970-01-01 03:04:05.010Z'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2)";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two > 1 AND clust_two < 3";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three >= timestamp '1970-01-01 03:04:05.010Z' AND clust_three <= timestamp '1970-01-01 03:04:05.020Z'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2) AND clust_three >= timestamp '1970-01-01 03:04:05.010Z' AND clust_three <= timestamp '1970-01-01 03:04:05.020Z'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two < 2";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two > 2";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two = 2";
        assertEquals(execute(sql).getRowCount(), 1);
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
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_1")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE KEYSPACE_1.TABLE_1 (COLUMN_1 bigint PRIMARY KEY)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_1"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_1")
                .build(), new Duration(1, MINUTES));
        assertContains(execute("SHOW COLUMNS FROM cassandra.keyspace_1.table_1"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("column_1", "bigint", "", "")
                .build());

        execute("INSERT INTO keyspace_1.table_1 (column_1) VALUES (1)");

        assertEquals(execute("SELECT column_1 FROM cassandra.keyspace_1.table_1").getRowCount(), 1);
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
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_2")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\" bigint PRIMARY KEY)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_2"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_2")
                .build(), new Duration(1, MINUTES));
        assertContains(execute("SHOW COLUMNS FROM cassandra.keyspace_2.table_2"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("column_2", "bigint", "", "")
                .build());

        execute("INSERT INTO \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\") VALUES (1)");

        assertEquals(execute("SELECT column_2 FROM cassandra.keyspace_2.table_2").getRowCount(), 1);
        assertUpdate("DROP TABLE cassandra.keyspace_2.table_2");

        // when an identifier is unquoted the lowercase and uppercase spelling may be used interchangeable
        session.execute("DROP KEYSPACE \"KEYSPACE_2\"");
    }

    @Test
    public void testKeyspaceNameAmbiguity()
    {
        // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 keyspaces with names
        // that have differences only in letters case.
        session.execute("CREATE KEYSPACE \"KeYsPaCe_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE KEYSPACE \"kEySpAcE_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");

        // Although in Trino all the schema and table names are always displayed as lowercase
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_3")
                .row("keyspace_3")
                .build(), new Duration(1, MINUTES));

        // There is no way to figure out what the exactly keyspace we want to retrieve tables from
        assertQueryFailsEventually(
                "SHOW TABLES FROM cassandra.keyspace_3",
                "More than one keyspace has been found for the case insensitive schema name: keyspace_3 -> \\(KeYsPaCe_3, kEySpAcE_3\\)",
                new Duration(1, MINUTES));

        session.execute("DROP KEYSPACE \"KeYsPaCe_3\"");
        session.execute("DROP KEYSPACE \"kEySpAcE_3\"");
    }

    @Test
    public void testTableNameAmbiguity()
    {
        session.execute("CREATE KEYSPACE keyspace_4 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_4")
                .build(), new Duration(1, MINUTES));

        // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 tables with names
        // that have differences only in letters case.
        session.execute("CREATE TABLE keyspace_4.\"TaBlE_4\" (column_4 bigint PRIMARY KEY)");
        session.execute("CREATE TABLE keyspace_4.\"tAbLe_4\" (column_4 bigint PRIMARY KEY)");

        // Although in Trino all the schema and table names are always displayed as lowercase
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_4"), resultBuilder(getSession(), createUnboundedVarcharType())
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
    }

    @Test
    public void testColumnNameAmbiguity()
    {
        session.execute("CREATE KEYSPACE keyspace_5 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_5")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE keyspace_5.table_5 (\"CoLuMn_5\" bigint PRIMARY KEY, \"cOlUmN_5\" bigint)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_5"), resultBuilder(getSession(), createUnboundedVarcharType())
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
    }

    @Test
    public void testUnsupportedColumnType()
    {
        // TODO currently all standard types are supported to some extent. We should add a test with custom type if possible.
    }

    @Test
    public void testNullAndEmptyTimestamp()
    {
        String tableName = "test_empty_timestamp";

        session.execute(format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, tableName));
        session.execute(format("CREATE TABLE %s.%s (id int PRIMARY KEY, timestamp_column_with_null timestamp, timestamp_column_with_empty timestamp)", KEYSPACE, tableName));
        session.execute(format("INSERT INTO %s.%s (id, timestamp_column_with_null, timestamp_column_with_empty) VALUES (1, NULL, '')", KEYSPACE, tableName));
        assertContainsEventually(() -> execute(format("SHOW TABLES FROM cassandra.%s LIKE '%s'", KEYSPACE, tableName)), resultBuilder(getSession(), createUnboundedVarcharType())
                .row(tableName)
                .build(), new Duration(1, MINUTES));

        assertThat(query(format("SELECT timestamp_column_with_null FROM %s.%s", KEYSPACE, tableName)))
                .matches("VALUES CAST(NULL AS timestamp(3) with time zone)");
        assertThat(query(format("SELECT timestamp_column_with_empty FROM %s.%s", KEYSPACE, tableName)))
                .matches("VALUES CAST(NULL AS timestamp(3) with time zone)");

        assertThat(query(format("SELECT id FROM %s.%s WHERE timestamp_column_with_null IS NULL", KEYSPACE, tableName)))
                .matches("VALUES 1");
        assertThat(query(format("SELECT id FROM %s.%s WHERE timestamp_column_with_empty IS NULL", KEYSPACE, tableName)))
                .matches("VALUES 1");

        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testEmptyTimestampClusteringKey()
    {
        String tableName = "test_empty_timestamp_clustering_key";

        session.execute(format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, tableName));
        session.execute(format("CREATE TABLE %s.%s (id int, timestamp_column_with_empty timestamp, PRIMARY KEY (id, timestamp_column_with_empty))", KEYSPACE, tableName));
        session.execute(format("INSERT INTO %s.%s (id, timestamp_column_with_empty) VALUES (1, '')", KEYSPACE, tableName));
        assertContainsEventually(() -> execute(format("SHOW TABLES FROM cassandra.%s LIKE '%s'", KEYSPACE, tableName)), resultBuilder(getSession(), createUnboundedVarcharType())
                .row(tableName)
                .build(), new Duration(1, MINUTES));

        assertThat(query(format("SELECT timestamp_column_with_empty FROM %s.%s", KEYSPACE, tableName)))
                .matches("VALUES CAST(NULL AS timestamp(3) with time zone)");

        assertThat(query(format("SELECT id FROM %s.%s WHERE timestamp_column_with_empty IS NULL", KEYSPACE, tableName)))
                .matches("VALUES 1");

        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testNestedCollectionType()
    {
        session.execute("CREATE KEYSPACE keyspace_test_nested_collection WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_test_nested_collection")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE keyspace_test_nested_collection.table_set (column_5 bigint PRIMARY KEY, nested_collection frozen<set<set<bigint>>>)");
        session.execute("CREATE TABLE keyspace_test_nested_collection.table_list (column_5 bigint PRIMARY KEY, nested_collection frozen<list<list<bigint>>>)");
        session.execute("CREATE TABLE keyspace_test_nested_collection.table_map (column_5 bigint PRIMARY KEY, nested_collection frozen<map<int, map<bigint, bigint>>>)");

        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_test_nested_collection"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_set")
                .row("table_list")
                .row("table_map")
                .build(), new Duration(1, MINUTES));

        session.execute("INSERT INTO keyspace_test_nested_collection.table_set (column_5, nested_collection) VALUES (1, {{1, 2, 3}})");
        assertEquals(execute("SELECT nested_collection FROM cassandra.keyspace_test_nested_collection.table_set").getMaterializedRows().get(0),
                new MaterializedRow(DEFAULT_PRECISION, "[[1,2,3]]"));

        session.execute("INSERT INTO keyspace_test_nested_collection.table_list (column_5, nested_collection) VALUES (1, [[4, 5, 6]])");
        assertEquals(execute("SELECT nested_collection FROM cassandra.keyspace_test_nested_collection.table_list").getMaterializedRows().get(0),
                new MaterializedRow(DEFAULT_PRECISION, "[[4,5,6]]"));

        session.execute("INSERT INTO keyspace_test_nested_collection.table_map (column_5, nested_collection) VALUES (1, {7:{8:9}})");
        assertEquals(execute("SELECT nested_collection FROM cassandra.keyspace_test_nested_collection.table_map").getMaterializedRows().get(0),
                new MaterializedRow(DEFAULT_PRECISION, "{7:{8:9}}"));

        session.execute("DROP KEYSPACE keyspace_test_nested_collection");
    }

    @Test
    public void testAllTypesInsert()
    {
        String sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                " FROM " + TABLE_ALL_TYPES_INSERT;
        assertEquals(execute(sql).getRowCount(), 0);

        // TODO Following types are not supported now. We need to change null into the value after fixing it
        // blob, frozen<set<type>>, inet, list<type>, map<type,type>, set<type>, decimal, varint
        // timestamp can be inserted but the expected and actual values are not same
        execute("INSERT INTO " + TABLE_ALL_TYPES_INSERT + " (" +
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
                "null, " +
                "'varchar1', " +
                "null, " +
                "UUID '50554d6e-29bb-11e5-b345-feff819cdc9f', " +
                "null, " +
                "null, " +
                "null " +
                ")");

        MaterializedResult result = execute(sql);
        int rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
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
                null,
                "varchar1",
                null,
                java.util.UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"),
                null,
                null,
                null));

        // insert null for all datatypes
        execute("INSERT INTO " + TABLE_ALL_TYPES_INSERT + " (" +
                "key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal," +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                ") VALUES (" +
                "'key2', null, null, null, null, null, null, null, null," +
                "null, null, null, null, null, null, null, null, null)");
        sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                " FROM " + TABLE_ALL_TYPES_INSERT + " WHERE key = 'key2'";
        result = execute(sql);
        rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key2", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));

        // insert into only a subset of columns
        execute("INSERT INTO " + TABLE_ALL_TYPES_INSERT + " (" +
                "key, typeinteger, typeansi, typeboolean) VALUES (" +
                "'key3', 999, 'ansi', false)");
        sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                " FROM " + TABLE_ALL_TYPES_INSERT + " WHERE key = 'key3'";
        result = execute(sql);
        rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key3", null, 999, null, null, null, "ansi", false, null, null, null, null, null, null, null, null, null, null));
    }

    @Test
    @Override
    public void testDelete()
    {
        String keyspaceAndTable = format("%s.%s", KEYSPACE, TABLE_DELETE_DATA);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable).getRowCount(), 15);

        // error
        assertThatThrownBy(() -> execute("DELETE FROM " + keyspaceAndTable))
                .isInstanceOf(RuntimeException.class);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable).getRowCount(), 15);

        String whereClusteringKeyOnly = " WHERE clust_one='clust_one_2'";
        assertThatThrownBy(() -> execute("DELETE FROM " + keyspaceAndTable + whereClusteringKeyOnly))
                .isInstanceOf(RuntimeException.class);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable).getRowCount(), 15);

        String whereMultiplePartitionKeyWithClusteringKey = " WHERE " +
                " (partition_one=1 AND partition_two=1 AND clust_one='clust_one_1') OR " +
                " (partition_one=1 AND partition_two=2 AND clust_one='clust_one_2') ";
        assertThatThrownBy(() -> execute("DELETE FROM " + keyspaceAndTable + whereMultiplePartitionKeyWithClusteringKey))
                .isInstanceOf(RuntimeException.class);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable).getRowCount(), 15);

        // success
        String wherePrimaryKey = " WHERE partition_one=3 AND partition_two=3 AND clust_one='clust_one_3'";
        execute("DELETE FROM " + keyspaceAndTable + wherePrimaryKey);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable).getRowCount(), 14);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable + wherePrimaryKey).getRowCount(), 0);

        String wherePartitionKey = " WHERE partition_one=2 AND partition_two=2";
        execute("DELETE FROM " + keyspaceAndTable + wherePartitionKey);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable).getRowCount(), 12);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable + wherePartitionKey).getRowCount(), 0);

        String whereMultiplePartitionKey = " WHERE (partition_one=1 AND partition_two=1) OR (partition_one=1 AND partition_two=2)";
        execute("DELETE FROM " + keyspaceAndTable + whereMultiplePartitionKey);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable).getRowCount(), 6);
        assertEquals(execute("SELECT * FROM " + keyspaceAndTable + whereMultiplePartitionKey).getRowCount(), 0);
    }

    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Override
    public void testDeleteWithVarcharPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    @Override
    public void testDeleteAllDataFromTable()
    {
        assertThatThrownBy(super::testDeleteAllDataFromTable)
                .hasStackTraceContaining("Deleting without partition key is not supported");
    }

    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining("Delete without primary key or partition key is not supported");
    }

    private void assertSelect(String tableName, boolean createdByTrino)
    {
        Type inetType = createdByTrino ? createUnboundedVarcharType() : createVarcharType(45);

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

        MaterializedResult result = execute(sql);

        int rowCount = result.getRowCount();
        assertEquals(rowCount, 9);
        assertEquals(result.getTypes(), ImmutableList.of(
                createUnboundedVarcharType(),
                UUID,
                INTEGER,
                BIGINT,
                VARBINARY,
                TIMESTAMP_WITH_TIME_ZONE,
                createUnboundedVarcharType(),
                BOOLEAN,
                DOUBLE,
                DOUBLE,
                REAL,
                inetType,
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
            assertEquals(sortedRows.get(rowNumber - 1), new MaterializedRow(DEFAULT_PRECISION,
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

    private MaterializedResult execute(String sql)
    {
        return getQueryRunner().execute(SESSION, sql);
    }
}
