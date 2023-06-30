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
package io.trino.tests.product.cassandra;

import io.airlift.units.Duration;
import io.trino.jdbc.Row;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.internal.query.CassandraQueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Consumer;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tests.product.TestGroups.CASSANDRA;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TpchTableResults.PRESTO_NATION_RESULT;
import static io.trino.tests.product.cassandra.CassandraTpchTableDefinitions.CASSANDRA_NATION;
import static io.trino.tests.product.cassandra.CassandraTpchTableDefinitions.CASSANDRA_SUPPLIER;
import static io.trino.tests.product.cassandra.DataTypesTableDefinition.CASSANDRA_ALL_TYPES;
import static io.trino.tests.product.cassandra.TestConstants.CONNECTOR_NAME;
import static io.trino.tests.product.cassandra.TestConstants.KEY_SPACE;
import static io.trino.tests.product.utils.QueryAssertions.assertContainsEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.JAVA_OBJECT;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSelect
        extends ProductTest
        implements RequirementsProvider
{
    private Configuration configuration;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        this.configuration = configuration;
        return compose(
                immutableTable(CASSANDRA_NATION),
                immutableTable(CASSANDRA_SUPPLIER),
                immutableTable(CASSANDRA_ALL_TYPES));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectNation()
    {
        String sql = format(
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM %s.%s.%s",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectWithEqualityFilterOnPartitioningKey()
    {
        String sql = format(
                "SELECT n_nationkey FROM %s.%s.%s WHERE n_nationkey = 0",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(0));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectWithFilterOnPartitioningKey()
    {
        String sql = format(
                "SELECT n_nationkey FROM %s.%s.%s WHERE n_nationkey > 23",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(24));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectWithEqualityFilterOnNonPartitioningKey()
    {
        String sql = format(
                "SELECT n_name FROM %s.%s.%s WHERE n_name = 'UNITED STATES'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("UNITED STATES"));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectWithNonEqualityFilterOnNonPartitioningKey()
    {
        String sql = format(
                "SELECT n_name FROM %s.%s.%s WHERE n_name < 'B'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("ALGERIA"), row("ARGENTINA"));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectWithMorePartitioningKeysThanLimit()
    {
        String sql = format(
                "SELECT s_suppkey FROM %s.%s.%s WHERE s_suppkey = 10",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_SUPPLIER.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(10));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectWithMorePartitioningKeysThanLimitNonPK()
    {
        String sql = format(
                "SELECT s_suppkey FROM %s.%s.%s WHERE s_name = 'Supplier#000000010'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_SUPPLIER.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row(10));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testAllDataTypes()
    {
        // NOTE: DECIMAL is treated like DOUBLE
        QueryResult query = onTrino().executeQuery(format(
                "SELECT a, b, bl, bo, d, do, dt, f, fr, i, integer, l, m, s, si, t, ti, ts, tu, u, v, vari FROM %s.%s.%s",
                CONNECTOR_NAME, KEY_SPACE, CASSANDRA_ALL_TYPES.getName()));

        assertThat(query)
                .hasColumns(VARCHAR, BIGINT, VARBINARY, BOOLEAN, DOUBLE, DOUBLE, DATE, REAL, VARCHAR, JAVA_OBJECT,
                        INTEGER, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR, TINYINT, TIMESTAMP_WITH_TIMEZONE, JAVA_OBJECT, JAVA_OBJECT,
                        VARCHAR, VARCHAR)
                .containsOnly(
                        row("\0",
                                Long.MIN_VALUE,
                                new byte[] {0},
                                false,
                                0f,
                                Double.MIN_VALUE,
                                Date.valueOf("1970-01-02"),
                                Float.MIN_VALUE,
                                "[0]",
                                "0.0.0.0",
                                Integer.MIN_VALUE,
                                "[0]",
                                "{\"\\u0000\":-2147483648,\"a\":0}",
                                "[0]",
                                Short.MIN_VALUE,
                                "\0",
                                Byte.MIN_VALUE,
                                Timestamp.from(OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3",
                                "01234567-0123-0123-0123-0123456789ab",
                                "\0",
                                String.valueOf(Long.MIN_VALUE)),
                        row("the quick brown fox jumped over the lazy dog",
                                9223372036854775807L,
                                "01234".getBytes(UTF_8),
                                true,
                                Double.valueOf("99999999999999999999999999999999999999"),
                                Double.MAX_VALUE,
                                Date.valueOf("9999-12-31"),
                                Float.MAX_VALUE,
                                "[4,5,6,7]",
                                "255.255.255.255",
                                Integer.MAX_VALUE,
                                "[4,5,6]",
                                "{\"a\":1,\"b\":2}",
                                "[4,5,6]",
                                Short.MAX_VALUE,
                                "this is a text value",
                                Byte.MAX_VALUE,
                                Timestamp.from(OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC).toInstant()),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3",
                                "01234567-0123-0123-0123-0123456789ab",
                                "abc",
                                String.valueOf(Long.MAX_VALUE)),
                        row("def", null, null, null, null, null, null, null, null, null, null,
                                null, null, null, null, null, null, null, null, null, null, null));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testNationJoinNation()
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, CASSANDRA_NATION.getName());
        String sql = format(
                "SELECT n1.n_name, n2.n_regionkey FROM %s n1 JOIN " +
                        "%s n2 ON n1.n_nationkey = n2.n_regionkey " +
                        "WHERE n1.n_nationkey=3",
                tableName,
                tableName);
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testNationJoinRegion()
    {
        String sql = format(
                "SELECT c.n_name, t.name FROM %s.%s.%s c JOIN " +
                        "tpch.tiny.region t ON c.n_regionkey = t.regionkey " +
                        "WHERE c.n_nationkey=3",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("CANADA", "AMERICA"));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectAllTypePartitioningMaterializedView()
    {
        String materializedViewName = format("%s_partitioned_mv", CASSANDRA_ALL_TYPES.getName());
        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, materializedViewName));
        onCassandra(format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE b IS NOT NULL PRIMARY KEY (a, b)",
                KEY_SPACE,
                materializedViewName,
                KEY_SPACE,
                CASSANDRA_ALL_TYPES.getName()));

        assertContainsEventually(() -> onTrino().executeQuery(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                onTrino().executeQuery(format("SELECT '%s'", materializedViewName)),
                new Duration(1, MINUTES));

        // Materialized view may not return all results during the creation
        assertContainsEventually(() -> onTrino().executeQuery(format("SELECT status_replicated FROM %s.system.built_views WHERE view_name = '%s'", CONNECTOR_NAME, materializedViewName)),
                onTrino().executeQuery("SELECT true"),
                new Duration(1, MINUTES));

        QueryResult query = onTrino().executeQuery(format(
                "SELECT a, b, bl, bo, d, do, dt, f, fr, i, integer, l, m, s, si, t, ti, ts, tu, u, v, vari FROM %s.%s.%s WHERE a = '\0'",
                CONNECTOR_NAME, KEY_SPACE, materializedViewName));

        assertThat(query)
                .hasColumns(VARCHAR, BIGINT, VARBINARY, BOOLEAN, DOUBLE, DOUBLE, DATE, REAL, VARCHAR, JAVA_OBJECT,
                        INTEGER, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR, TINYINT, TIMESTAMP_WITH_TIMEZONE, JAVA_OBJECT, JAVA_OBJECT,
                        VARCHAR, VARCHAR)
                .containsOnly(
                        row("\0",
                                Long.MIN_VALUE,
                                new byte[] {0},
                                false,
                                0f,
                                Double.MIN_VALUE,
                                Date.valueOf("1970-01-02"),
                                Float.MIN_VALUE,
                                "[0]",
                                "0.0.0.0",
                                Integer.MIN_VALUE,
                                "[0]",
                                "{\"\\u0000\":-2147483648,\"a\":0}",
                                "[0]",
                                Short.MIN_VALUE,
                                "\0",
                                Byte.MIN_VALUE,
                                Timestamp.from(OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()),
                                "d2177dd0-eaa2-11de-a572-001b779c76e3",
                                "01234567-0123-0123-0123-0123456789ab",
                                "\0",
                                String.valueOf(Long.MIN_VALUE)));

        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, materializedViewName));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectClusteringMaterializedView()
    {
        String mvName = "clustering_mv";
        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, mvName));
        onCassandra(format("CREATE MATERIALIZED VIEW %s.%s AS " +
                        "SELECT * FROM %s.%s " +
                        "WHERE s_nationkey IS NOT NULL " +
                        "PRIMARY KEY (s_nationkey, s_suppkey) " +
                        "WITH CLUSTERING ORDER BY (s_nationkey DESC)",
                KEY_SPACE,
                mvName,
                KEY_SPACE,
                CASSANDRA_SUPPLIER.getName()));

        assertContainsEventually(() -> onTrino().executeQuery(format("SHOW TABLES FROM %s.%s", CONNECTOR_NAME, KEY_SPACE)),
                onTrino().executeQuery(format("SELECT '%s'", mvName)),
                new Duration(1, MINUTES));

        // Materialized view may not return all results during the creation
        assertContainsEventually(() -> onTrino().executeQuery(format("SELECT status_replicated FROM %s.system.built_views WHERE view_name = '%s'", CONNECTOR_NAME, mvName)),
                onTrino().executeQuery("SELECT true"),
                new Duration(1, MINUTES));

        QueryResult aggregateQueryResult = onTrino()
                .executeQuery(format(
                        "SELECT MAX(s_nationkey), SUM(s_suppkey), AVG(s_acctbal) " +
                                "FROM %s.%s.%s WHERE s_suppkey BETWEEN 1 AND 10 ", CONNECTOR_NAME, KEY_SPACE, mvName));
        assertThat(aggregateQueryResult).containsOnly(
                row(24, 55, 4334.653));

        QueryResult orderedResult = onTrino()
                .executeQuery(format(
                        "SELECT s_nationkey, s_suppkey, s_acctbal " +
                                "FROM %s.%s.%s ORDER BY s_nationkey LIMIT 1", CONNECTOR_NAME, KEY_SPACE, mvName));
        assertThat(orderedResult).containsOnly(
                row(0, 24, 9170.71));

        onCassandra(format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEY_SPACE, mvName));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testProtocolVersion()
    {
        QueryResult queryResult = onTrino()
                .executeQuery(format("SELECT native_protocol_version FROM %s.system.local", CONNECTOR_NAME));
        assertThat(queryResult).containsOnly(row("4"));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectTupleType()
    {
        String tableName = "select_tuple_table";
        onCassandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));

        onCassandra(format("CREATE TABLE %s.%s (key int, value frozen<tuple<int, text, float>>, PRIMARY KEY (key))",
                 KEY_SPACE, tableName));

        onCassandra(format("INSERT INTO %s.%s (key, value) VALUES(1, (1, 'text-1', 1.11))", KEY_SPACE, tableName));

        QueryResult queryResult = onTrino().executeQuery(
                 format("SELECT * FROM %s.%s.%s", CONNECTOR_NAME, KEY_SPACE, tableName));
        assertThat(queryResult).hasRowsCount(1);
        Assertions.assertThat(queryResult.row(0).get(0)).isEqualTo(1);
        Assertions.assertThat(queryResult.row(0).get(1)).isEqualTo(Row.builder()
                .addUnnamedField(1)
                .addUnnamedField("text-1")
                .addUnnamedField(1.11f)
                .build());

        onCassandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectTupleTypeInPrimaryKey()
    {
        String tableName = "select_tuple_in_primary_key_table";
        onCassandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));

        onCassandra(format("CREATE TABLE %s.%s (intkey int, tuplekey frozen<tuple<int, text, float>>, PRIMARY KEY (intkey, tuplekey))",
                KEY_SPACE, tableName));

        onCassandra(format("INSERT INTO %s.%s (intkey, tuplekey) VALUES(1, (1, 'text-1', 1.11))", KEY_SPACE, tableName));

        Consumer<QueryResult> assertion = queryResult -> {
            assertThat(queryResult).hasRowsCount(1);
            Assertions.assertThat(queryResult.row(0).get(0)).isEqualTo(1);
            Assertions.assertThat(queryResult.row(0).get(1)).isEqualTo(Row.builder()
                    .addUnnamedField(1)
                    .addUnnamedField("text-1")
                    .addUnnamedField(1.11f)
                    .build());
        };
        assertion.accept(onTrino().executeQuery(format("SELECT * FROM %s.%s.%s", CONNECTOR_NAME, KEY_SPACE, tableName)));
        assertion.accept(onTrino().executeQuery(format("SELECT * FROM %s.%s.%s WHERE intkey = 1 and tuplekey = row(1, 'text-1', 1.11)", CONNECTOR_NAME, KEY_SPACE, tableName)));

        onCassandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectUserDefinedType()
    {
        String udtName = "type_user_defined";
        String tableName = "user_defined_type_table";

        onCassandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));
        onCassandra(format("DROP TYPE IF EXISTS %s.%s", KEY_SPACE, udtName));

        onCassandra(format("" +
                "CREATE TYPE %s.%s (" +
                "t text, " +
                "u uuid, " +
                "integer int, " +
                "b bigint, " +
                "bl blob, " +
                "ts timestamp, " +
                "a ascii, " +
                "bo boolean, " +
                "d decimal, " +
                "do double, " +
                "f float, " +
                "i inet, " +
                "v varchar, " +
                "vari varint, " +
                "tu timeuuid, " +
                "l frozen <list<text>>, " +
                "m frozen <map<varchar, bigint>>, " +
                "s frozen <set<boolean>> " +
                ")", KEY_SPACE, udtName));

        onCassandra(format("" +
                "CREATE TABLE %s.%s (" +
                "key text PRIMARY KEY, " +
                "typeudt frozen <%s>, " +
                ")", KEY_SPACE, tableName, udtName));

        onCassandra(format("" +
                "INSERT INTO %s.%s (key, typeudt) VALUES (" +
                "'key'," +
                "{ " +
                "t: 'text', " +
                "u: 01234567-0123-0123-0123-0123456789ab, " +
                "integer: -2147483648, " +
                "b:  -9223372036854775808," +
                "bl: 0x3031323334, " +
                "ts: '1970-01-01 13:30:00.000', " +
                "a: 'ansi', " +
                "bo: true, " +
                "d: 99999999999999997748809823456034029568, " +
                "do: 4.9407e-324, " +
                "f: 1.4013e-45, " +
                "i: '0.0.0.0', " +
                "v: 'varchar', " +
                "vari: -9223372036854775808, " +
                "tu: d2177dd0-eaa2-11de-a572-001b779c76e3, " +
                "l: ['list'], " +
                "m: {'map': 1}, " +
                "s: {true} " +
                "}" +
                ")", KEY_SPACE, tableName));

        QueryResult queryResult = onTrino().executeQuery(format("" +
                "SELECT " +
                "typeudt.t, " +
                "typeudt.u, " +
                "typeudt.integer, " +
                "typeudt.b, " +
                "typeudt.bl, " +
                "typeudt.ts, " +
                "typeudt.a, " +
                "typeudt.bo, " +
                "typeudt.d, " +
                "typeudt.do, " +
                "typeudt.f, " +
                "typeudt.i, " +
                "typeudt.v, " +
                "typeudt.vari, " +
                "typeudt.tu, " +
                "typeudt.l, " +
                "typeudt.m, " +
                "typeudt.s " +
                "FROM %s.%s.%s", CONNECTOR_NAME, KEY_SPACE, tableName));
        assertThat(queryResult).containsOnly(row(
                "text",
                "01234567-0123-0123-0123-0123456789ab",
                -2147483648,
                -9223372036854775808L,
                "01234".getBytes(UTF_8),
                Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 13, 30)),
                "ansi",
                true,
                99999999999999997748809823456034029568D,
                4.9407e-324,
                1.4E-45f,
                "0.0.0.0",
                "varchar",
                "-9223372036854775808",
                "d2177dd0-eaa2-11de-a572-001b779c76e3",
                "[\"list\"]",
                "{\"map\":1}",
                "[true]"));

        onCassandra(format("DROP TABLE %s.%s", KEY_SPACE, tableName));
        onCassandra(format("DROP TYPE %s.%s", KEY_SPACE, udtName));
    }

    @Test(groups = {CASSANDRA, PROFILE_SPECIFIC_TESTS})
    public void testSelectUserDefinedTypeInPrimaryKey()
    {
        String udtName = "type_user_defined_primary_key";
        String tableName = "select_udt_in_primary_key_table";

        onCassandra(format("DROP TABLE IF EXISTS %s.%s", KEY_SPACE, tableName));
        onCassandra(format("DROP TYPE IF EXISTS %s.%s", KEY_SPACE, udtName));

        onCassandra(format("CREATE TYPE %s.%s (field1 text)", KEY_SPACE, udtName));
        onCassandra(format("CREATE TABLE %s.%s (intkey int, udtkey frozen<%s>, PRIMARY KEY (intkey, udtkey))", KEY_SPACE, tableName, udtName));

        onCassandra(format("INSERT INTO %s.%s (intkey, udtkey) VALUES(1, {field1: 'udt-1'})", KEY_SPACE, tableName));

        Consumer<QueryResult> assertion = queryResult -> {
            assertThat(queryResult).hasRowsCount(1);
            Assertions.assertThat(queryResult.row(0).get(0)).isEqualTo(1);
            Assertions.assertThat(queryResult.row(0).get(1)).isEqualTo(Row.builder()
                    .addField("field1", "udt-1")
                    .build());
        };
        assertion.accept(onTrino().executeQuery(format("SELECT * FROM %s.%s.%s", CONNECTOR_NAME, KEY_SPACE, tableName)));
        assertion.accept(onTrino().executeQuery(format("SELECT * FROM %s.%s.%s WHERE intkey = 1 AND udtkey = CAST(ROW('udt-1') AS ROW(x VARCHAR))", CONNECTOR_NAME, KEY_SPACE, tableName)));

        onCassandra(format("DROP TABLE %s.%s", KEY_SPACE, tableName));
        onCassandra(format("DROP TYPE %s.%s", KEY_SPACE, udtName));
    }

    private void onCassandra(String query)
    {
        try (CassandraQueryExecutor queryExecutor = new CassandraQueryExecutor(configuration)) {
            queryExecutor.executeQuery(query);
        }
    }
}
