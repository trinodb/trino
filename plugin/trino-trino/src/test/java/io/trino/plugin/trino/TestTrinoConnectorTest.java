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
package io.trino.plugin.trino;

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the read-only Trino connector against a remote in-process Trino instance.
 *
 * <p>The base JDBC, type mapping, and integration assertions share one local/remote
 * query-runner pair.
 */
final class TestTrinoConnectorTest
        extends BaseJdbcConnectorTest
{
    private DistributedQueryRunner remoteRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remoteRunner = TrinoQueryRunner.createRemoteQueryRunner();
        try {
            createRemoteFixtures();
            return TrinoQueryRunner.builder(remoteRunner)
                    .setRemoteCatalog("memory")
                    .setDefaultSchema("default")
                    .withComplexTypeTestData()
                    .withRemoteTpchCatalog()
                    .withStatisticsDisabledCatalog()
                    .build();
        }
        catch (Exception | Error failure) {
            TrinoQueryRunner.closeOnFailure(remoteRunner, failure);
            throw failure;
        }
    }

    private void createRemoteFixtures()
    {
        // Pre-populate remote memory catalog with tpch tables as read-only test fixtures.
        // BaseConnectorTest expects nation/region/orders/customer/lineitem/part/partsupp/supplier
        // to exist and be queryable through the connector's default catalog+schema.
        TrinoQueryRunner.populateTpchData(remoteRunner);
        Session remoteSession = remoteMemorySession();
        remoteRunner.execute(
                remoteSession,
                "CREATE TABLE simple_table AS SELECT * FROM (VALUES BIGINT '1', BIGINT '2') AS t(col)");
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE native_query_unsupported AS
                SELECT
                    CAST(1 AS BIGINT) AS one,
                    CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)) AS two,
                    CAST('ok' AS VARCHAR) AS three
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_decimal_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('low', CAST(10.125 AS DECIMAL(10, 3))),
                        ('high', CAST(123.456 AS DECIMAL(10, 3)))
                ) AS t(id, amount)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_timestamp12_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('before', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789011' AS TIMESTAMP(12))),
                        ('after', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)))
                ) AS t(id, ts_col)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_timestamptz12_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('before', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789011 UTC' AS TIMESTAMP(12) WITH TIME ZONE)),
                        ('after', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012 UTC' AS TIMESTAMP(12) WITH TIME ZONE))
                ) AS t(id, ts_tz_col)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_interval_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('short', INTERVAL '1' DAY),
                        ('long', INTERVAL '2' DAY)
                ) AS t(id, duration)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_interval_ym_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('one_year', INTERVAL '12' MONTH),
                        ('fourteen_months', INTERVAL '14' MONTH)
                ) AS t(id, duration)
                """);
        remoteRunner.execute(remoteSession,
                """
                CREATE TABLE test_timetz_filter_pushdown AS
                SELECT * FROM (
                    VALUES
                        ('early', CAST(TIME '10:30:45.123 +09:00' AS TIME(3) WITH TIME ZONE)),
                        ('late', CAST(TIME '10:30:45.124 +09:00' AS TIME(3) WITH TIME ZONE))
                ) AS t(id, time_tz_col)
                """);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        Session remoteSession = remoteMemorySession();
        return sql -> remoteRunner.execute(remoteSession, sql);
    }

    @Override
    protected TestTable newTrinoTable(String namePrefix, String tableDefinition, List<String> rowsToInsert)
    {
        // The connector is read-only: fixtures that base tests would create through
        // the connector are created directly on the remote instead. The connector's
        // default schema maps to the same remote schema, so the tables stay visible.
        return new TestTable(onRemoteDatabase(), namePrefix, tableDefinition, rowsToInsert);
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // the default AUTOMATIC strategy requires statistics-based benefit
                // for every base test case; force pushdown like other JDBC suites
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    private Session remoteMemorySession()
    {
        return testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();
    }

    @Override
    protected void assertQueryFails(String sql, String expectedMessageRegex)
    {
        super.assertQueryFails(sql, readOnlyFailurePattern(expectedMessageRegex));
    }

    @Override
    protected void assertQueryFails(Session session, String sql, String expectedMessageRegex)
    {
        super.assertQueryFails(session, sql, readOnlyFailurePattern(expectedMessageRegex));
    }

    private static String readOnlyFailurePattern(String expectedMessageRegex)
    {
        if (expectedMessageRegex.startsWith("This connector does not support")) {
            return "(?s)(?:" + expectedMessageRegex + "|Access Denied:.*)";
        }
        return expectedMessageRegex;
    }

    // =========================================================================
    // Connector behavior declarations -- read-only connector
    // =========================================================================

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            // Read support for complex types
            case SUPPORTS_ARRAY,
                 SUPPORTS_ROW_TYPE -> true;

            // Pushdown: both sides are Trino with identical SQL syntax
            case SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT -> true;
            // Remote TopN is still applied, but transport projection can wrap it in
            // an outer query, so local ordering verification must remain in the plan.
            case SUPPORTS_TOPN_PUSHDOWN -> false;
            case SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 // IS [NOT] DISTINCT FROM and varchar inequality conditions render
                 // through the delegation path; both sides share Trino semantics
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY -> true;
            case SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN -> true;
            // Advanced statistical aggregation functions not yet implemented
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION -> false;

            // Read-only connector: all write, DDL, and schema operations
            // are blocked in TrinoClient with NOT_SUPPORTED errors
            case SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_INSERT,
                 SUPPORTS_MULTI_STATEMENT_WRITES -> false;
            case SUPPORTS_DELETE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_ROW_LEVEL_UPDATE,
                 SUPPORTS_MERGE,
                 SUPPORTS_TRUNCATE -> false;
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS -> false;
            case SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_COLUMN -> false;
            case SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_DROP_SCHEMA_CASCADE -> false;
            case SUPPORTS_NOT_NULL_CONSTRAINT -> false;
            case SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    // =========================================================================
    // Architectural overrides -- federation SQL/format differences
    // =========================================================================

    @Test
    @Override
    public void testShowCreateTable()
    {
        String showCreate = computeActual("SHOW CREATE TABLE nation").getOnlyValue().toString();
        assertThat(showCreate)
                .contains("CREATE TABLE")
                .contains("nationkey")
                .contains("name")
                .contains("regionkey");
    }

    // =========================================================================
    // Architectural overrides -- query passthrough SQL resolution
    //
    // The system.query() table function resolves SQL on the remote Trino,
    // which requires catalog-qualified table names. The framework tests use
    // unqualified names that fail through federation.
    // Passthrough is verified by the integration tests later in this class.
    // =========================================================================

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        // Override: use catalog-qualified SQL for the remote Trino
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT 1 AS col'))"))
                .matches("VALUES 1");
    }

    @Test
    @Override
    public void testNativeQueryParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();

        assertQuery(session, "EXECUTE my_query_simple USING 'SELECT 1 a'", "VALUES 1");
        assertQuery(session, "EXECUTE my_query USING 'name', 'memory.default.nation WHERE nationkey = 0'", "VALUES 'ALGERIA'");
    }

    @Test
    @Override
    public void testNativeQuerySelectFromNation()
    {
        // Override: use catalog-qualified table name on the remote
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT name FROM memory.default.nation WHERE nationkey = 0'))"))
                .matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
    }

    @Test
    void testNativeQueryAnonymousOutputColumn()
    {
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT count(*) FROM memory.default.nation'))"))
                .matches("VALUES BIGINT '25'");
    }

    @Test
    @Override
    public void testNativeQuerySelectFromTestTable()
    {
        assertQuery(
                "SELECT * FROM TABLE(system.query(query => 'SELECT * FROM memory.default.simple_table'))",
                "VALUES 1, 2");
    }

    @Test
    @Override
    public void testNativeQueryColumnAlias()
    {
        // Override: use catalog-qualified table name on the remote
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT name AS nation_name FROM memory.default.nation WHERE nationkey = 0'))"))
                .matches("VALUES CAST('ALGERIA' AS VARCHAR(25))");
    }

    @Test
    @Override
    public void testNativeQueryColumnAliasNotFound()
    {
        assertQueryFails(
                "SELECT name FROM TABLE(system.query(query => 'SELECT name AS region_name FROM memory.default.region'))",
                ".* Column 'name' cannot be resolved");
        assertQueryFails(
                "SELECT column_not_found FROM TABLE(system.query(query => 'SELECT name AS region_name FROM memory.default.region'))",
                ".* Column 'column_not_found' cannot be resolved");
    }

    @Test
    @Override
    public void testNativeQuerySelectUnsupportedType()
    {
        assertQuery(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'default' AND table_name = 'native_query_unsupported'",
                "VALUES 'one', 'two', 'three'");

        assertQuery(
                "SELECT typeof(two) FROM TABLE(system.query(query => 'SELECT one, two, three FROM memory.default.native_query_unsupported'))",
                "VALUES 'timestamp(12)'");
        assertQuery(
                "SELECT one, CAST(two AS VARCHAR), three FROM TABLE(system.query(query => 'SELECT one, two, three FROM memory.default.native_query_unsupported'))",
                "VALUES (1, '2024-01-15 10:30:45.123456789012', 'ok')");
    }

    @Test
    void testNativeQuerySelectSketchType()
    {
        assertQuery(
                "SELECT typeof(x) FROM TABLE(system.query(query => 'SELECT approx_set(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)'))",
                "VALUES 'HyperLogLog'");
        assertQuery(
                "SELECT cardinality(CAST(CAST(x AS VARBINARY) AS HyperLogLog)) FROM TABLE(system.query(query => 'SELECT approx_set(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)'))",
                "VALUES 5");
    }

    @Test
    void testDecimalPredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_decimal_filter_pushdown WHERE amount > DECIMAL '50.000'"))
                .isFullyPushedDown()
                .matches("VALUES 'high'");
    }

    @Test
    void testTimestamp12PredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_timestamp12_filter_pushdown WHERE ts_col > TIMESTAMP '2024-01-15 10:30:45.123456789011'"))
                .isFullyPushedDown()
                .matches("VALUES CAST('after' AS VARCHAR(6))");
    }

    @Test
    void testTimestampWithTimeZone12PredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_timestamptz12_filter_pushdown WHERE ts_tz_col > TIMESTAMP '2024-01-15 10:30:45.123456789011 UTC'"))
                .isFullyPushedDown()
                .matches("VALUES CAST('after' AS VARCHAR(6))");
    }

    @Test
    void testIntervalPredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_interval_filter_pushdown WHERE duration > INTERVAL '1' DAY"))
                .isFullyPushedDown()
                .matches("VALUES CAST('long' AS VARCHAR(5))");
        assertThat(query("SELECT id FROM test_interval_filter_pushdown WHERE duration > INTERVAL '-1' DAY"))
                .isFullyPushedDown()
                .matches("VALUES CAST('short' AS VARCHAR(5)), CAST('long' AS VARCHAR(5))");
    }

    @Test
    void testIntervalYearToMonthPredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_interval_ym_filter_pushdown WHERE duration > INTERVAL '12' MONTH"))
                .isFullyPushedDown()
                .matches("VALUES CAST('fourteen_months' AS VARCHAR(15))");
    }

    @Test
    void testTimeWithTimeZonePredicatePushdown()
    {
        assertThat(query("SELECT id FROM test_timetz_filter_pushdown WHERE time_tz_col > TIME '10:30:45.123 +09:00'"))
                .isFullyPushedDown()
                .matches("VALUES CAST('late' AS VARCHAR(5))");
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        assertPassthroughStatementRejected("CREATE TABLE memory.default.native_query_create AS SELECT 1 AS value");
        assertThat(computeRemoteActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain("native_query_create");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        assertPassthroughStatementRejected("INSERT INTO memory.default.native_query_missing VALUES (1)");
        assertThat(computeRemoteActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain("native_query_missing");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        assertPassthroughStatementRejected("INSERT INTO memory.default.nation VALUES (99, 'TEST', 0, 'test')");
        assertThat(computeRemoteActual("SELECT count(*) FROM nation WHERE nationkey = 99").getOnlyValue())
                .isEqualTo(0L);
    }

    @Test
    void testNativeQueryDeleteStatement()
    {
        assertPassthroughStatementRejected("DELETE FROM memory.default.nation WHERE nationkey = 0");
        assertThat(computeRemoteActual("SELECT count(*) FROM nation WHERE nationkey = 0").getOnlyValue())
                .isEqualTo(1L);
    }

    @Test
    void testNativeQueryUpdateStatement()
    {
        assertPassthroughStatementRejected("UPDATE memory.default.nation SET name = 'X' WHERE nationkey = 0");
        assertThat(computeRemoteActual("SELECT name FROM nation WHERE nationkey = 0").getOnlyValue())
                .isEqualTo("ALGERIA");
    }

    @Test
    void testNativeQueryCallStatement()
    {
        // Only the local rejection is asserted: there is no observable remote
        // procedure, so a remote-state invariant would be vacuous here
        assertPassthroughStatementRejected("CALL system.runtime.kill_query('query-id', 'reason')");
    }

    private MaterializedResult computeRemoteActual(String sql)
    {
        return remoteRunner.execute(remoteMemorySession(), sql);
    }

    @Test
    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        // The passthrough validator parses the statement locally before any
        // remote contact, so the syntax error comes from the local parser,
        // not the remote cluster.
        assertThatThrownBy(() -> computeActual(
                "SELECT * FROM TABLE(system.query(query => 'SOME INCORRECT SYNTAX'))"))
                .hasMessageContaining("mismatched input");
    }

    // =========================================================================
    // Architectural overrides -- type compatibility through federation
    // =========================================================================

    private void assertPassthroughStatementRejected(String sql)
    {
        assertThatThrownBy(() -> computeActual("SELECT * FROM TABLE(system.query(query => '" + sql.replace("'", "''") + "'))"))
                .hasMessageContaining("system.query only supports row-returning read queries");
    }

    // =========================================================================
    // Architectural overrides -- procedure and runner constraints
    // =========================================================================

    // The procedure is inherited from base-jdbc, but this connector exposes a
    // read-only surface and denies it before the remote SQL is executed.
    @Test
    @Override
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = "memory.default." + tableName;

        assertExecuteProcedureDenied("CALL system.execute('CREATE TABLE " + schemaTableName + " (a int)')");
        assertThat(computeRemoteActual("SHOW TABLES FROM memory.default LIKE '" + tableName + "'").getRowCount()).isEqualTo(0);
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = "memory.default." + tableName;

        assertExecuteProcedureDenied("CALL system.execute(query => 'CREATE TABLE " + schemaTableName + " (a int)')");
        assertThat(computeRemoteActual("SHOW TABLES FROM memory.default LIKE '" + tableName + "'").getRowCount()).isEqualTo(0);
    }

    @Test
    @Override
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertExecuteProcedureDenied("CALL system.execute('some incorrect syntax')");
    }

    @Test
    void testFlushMetadataCacheProcedure()
    {
        assertUpdate("CALL system.flush_metadata_cache()");
    }

    @Test
    void testDropNotNullConstraintDoesNotMutateRemoteTable()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_not_null", "(value bigint NOT NULL)")) {
            assertThatThrownBy(() -> computeActual("ALTER TABLE " + table.getName() + " ALTER COLUMN value DROP NOT NULL"))
                    .hasMessageContaining("Access Denied");
            assertThat(computeRemoteActual("SELECT is_nullable FROM information_schema.columns " +
                    "WHERE table_schema = 'default' AND table_name = '" + table.getName() + "' AND column_name = 'value'").getOnlyValue())
                    .isEqualTo("NO");
        }
    }

    private void assertExecuteProcedureDenied(String sql)
    {
        assertThatThrownBy(() -> computeActual(sql))
                .hasMessageContaining("Access Denied")
                .hasMessageContaining("Cannot execute procedure system.execute");
    }

    // Type mapping coverage

    // =========================================================================
    // Representative sample/high/null value coverage per type
    // =========================================================================

    @Test
    void testRepresentativeDataMapping()
    {
        // Replaces the coverage of the base testDataMappingSmokeTest, which requires
        // CREATE TABLE through the connector and is skipped for read-only connectors.
        // One wide fixture keeps the same sample/high/null and predicate coverage in
        // six remote queries instead of creating and querying one table per type.
        String columns = TrinoQueryRunner.DATA_MAPPING_CASES.stream()
                .map(TrinoQueryRunner.DataMappingCase::columnName)
                .collect(joining(", "));

        assertThat(query("SELECT " + columns + " FROM remote.default.data_mapping WHERE id = 1"))
                .matches(dataMappingExpectedRow(TrinoQueryRunner.DataMappingCase::sampleValue));
        assertThat(query("SELECT " + columns + " FROM remote.default.data_mapping WHERE id = 2"))
                .matches(dataMappingExpectedRow(TrinoQueryRunner.DataMappingCase::highValue));
        assertThat(query("SELECT " + columns + " FROM remote.default.data_mapping WHERE id = 3"))
                .matches(dataMappingExpectedRow(TrinoQueryRunner.DataMappingCase::nullValue));

        assertQuery(
                dataMappingPredicateQuery(dataMappingCase -> dataMappingCase.columnName() + " = " + dataMappingCase.sampleValue()),
                dataMappingPredicateExpectedRows(1));
        assertQuery(
                dataMappingPredicateQuery(dataMappingCase -> dataMappingCase.columnName() + " = " + dataMappingCase.highValue()),
                dataMappingPredicateExpectedRows(2));
        assertQuery(
                dataMappingPredicateQuery(dataMappingCase -> dataMappingCase.columnName() + " IS NULL"),
                dataMappingPredicateExpectedRows(3));
    }

    private static String dataMappingExpectedRow(Function<TrinoQueryRunner.DataMappingCase, String> value)
    {
        return "VALUES (" + TrinoQueryRunner.DATA_MAPPING_CASES.stream()
                .map(value)
                .collect(joining(", ")) + ")";
    }

    private static String dataMappingPredicateQuery(Function<TrinoQueryRunner.DataMappingCase, String> predicate)
    {
        return "SELECT type_name, id FROM (" + TrinoQueryRunner.DATA_MAPPING_CASES.stream()
                .map(dataMappingCase -> "SELECT '" + dataMappingCase.suffix() + "' AS type_name, id " +
                        "FROM remote.default.data_mapping WHERE " + predicate.apply(dataMappingCase))
                .collect(joining(" UNION ALL ")) + ")";
    }

    private static String dataMappingPredicateExpectedRows(int id)
    {
        return "VALUES " + TrinoQueryRunner.DATA_MAPPING_CASES.stream()
                .map(dataMappingCase -> "('" + dataMappingCase.suffix() + "', " + id + ")")
                .collect(joining(", "));
    }

    @Test
    void testTemporalTransportAcrossSessionTimeZones()
    {
        // Temporal transport decodes timestamps from strings; the decoded value must
        // not depend on the session zone. UTC + a non-hour offset (Kathmandu) + a DST
        // zone (Warsaw) form the minimal defense line.
        for (String zone : List.of("UTC", "Asia/Kathmandu", "Europe/Warsaw")) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zone))
                    .build();

            assertThat(query(session,
                    """
                    SELECT
                        sample.value_timestamp_12,
                        high.value_timestamp_12,
                        sample.value_timestamptz_12,
                        high.value_timestamptz_12,
                        sample.value_timestamptz_3,
                        timezone(sample.value_timestamptz_3),
                        timezone(sample.value_timestamptz_12),
                        sample.value_time_12,
                        CAST(timetz.x AS VARCHAR)
                    FROM remote.default.data_mapping sample
                    CROSS JOIN remote.default.data_mapping high
                    CROSS JOIN remote.default.test_timetz3 timetz
                    WHERE sample.id = 1 AND high.id = 2
                    """))
                    .matches(
                            """
                            VALUES (
                                CAST(TIMESTAMP '2020-02-12 15:03:00.123456789012' AS timestamp(12)),
                                CAST(TIMESTAMP '2199-12-31 23:59:59.999999999999' AS timestamp(12)),
                                CAST(TIMESTAMP '2020-02-12 15:03:00.123456789012 +01:00' AS timestamp(12) with time zone),
                                CAST(TIMESTAMP '9999-12-31 23:59:59.999999999999 +12:00' AS timestamp(12) with time zone),
                                CAST(TIMESTAMP '2020-02-12 15:03:00.123 +01:00' AS timestamp(3) with time zone),
                                CAST('+01:00' AS varchar),
                                CAST('+01:00' AS varchar),
                                CAST(TIME '15:03:00.123456789012' AS time(12)),
                                CAST('10:30:45.123+09:00' AS varchar))
                            """);
        }

        // Typed-bind predicate leg: one representative zone is enough — bound values
        // carry their own zone/offset, so binding is session-zone independent
        Session kathmandu = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Kathmandu"))
                .build();
        assertQuery(
                kathmandu,
                """
                SELECT id
                FROM remote.default.data_mapping
                WHERE value_timestamp_12 = TIMESTAMP '2020-02-12 15:03:00.123456789012'
                    AND value_timestamptz_12 = TIMESTAMP '2020-02-12 15:03:00.123456789012 +01:00'
                    AND value_timestamptz_3 = TIMESTAMP '2020-02-12 15:03:00.123 +01:00'
                """,
                "VALUES 1");
    }

    @Test
    void testTimestampWithTimeZoneDstFold()
    {
        String expected =
                """
                VALUES
                    (
                        1,
                        CAST(at_timezone(TIMESTAMP '2022-10-30 00:30:00.123 UTC', 'Europe/Warsaw') AS TIMESTAMP(3) WITH TIME ZONE),
                        CAST('Europe/Warsaw' AS VARCHAR),
                        CAST(at_timezone(TIMESTAMP '2022-10-30 00:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE),
                        CAST('Europe/Warsaw' AS VARCHAR)
                    ),
                    (
                        2,
                        CAST(at_timezone(TIMESTAMP '2022-10-30 01:30:00.123 UTC', 'Europe/Warsaw') AS TIMESTAMP(3) WITH TIME ZONE),
                        CAST('Europe/Warsaw' AS VARCHAR),
                        CAST(at_timezone(TIMESTAMP '2022-10-30 01:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE),
                        CAST('Europe/Warsaw' AS VARCHAR)
                    )
                """;

        for (String zone : List.of("UTC", "Asia/Kathmandu", "Europe/Warsaw")) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zone))
                    .build();
            assertThat(query(session,
                    "SELECT id, p3, timezone(p3), p12, timezone(p12) " +
                            "FROM remote.default.test_tstz_dst_fold ORDER BY id"))
                    .matches(expected);
        }

        assertThat(query(
                "SELECT id FROM remote.default.test_tstz_dst_fold " +
                        "WHERE p3 = at_timezone(TIMESTAMP '2022-10-30 01:30:00.123 UTC', 'Europe/Warsaw')"))
                .isFullyPushedDown()
                .matches("VALUES 2");
        assertThat(query(
                "SELECT id FROM remote.default.test_tstz_dst_fold " +
                        "WHERE p12 = at_timezone(TIMESTAMP '2022-10-30 01:30:00.123456789012 UTC', 'Europe/Warsaw')"))
                .isFullyPushedDown()
                .matches("VALUES 2");
    }

    @Test
    void testNestedTimestampWithTimeZoneDstFold()
    {
        String earlier = "CAST(at_timezone(TIMESTAMP '2022-10-30 00:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE)";
        String later = "CAST(at_timezone(TIMESTAMP '2022-10-30 01:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE)";

        assertThat(query(
                "SELECT value, timezone(value) FROM remote.default.test_nested_tstz_dst_fold " +
                        "CROSS JOIN UNNEST(array_value) WITH ORDINALITY AS t(value, position) ORDER BY position"))
                .ordered()
                .matches("VALUES (" + earlier + ", CAST('Europe/Warsaw' AS VARCHAR)), (" + later + ", CAST('Europe/Warsaw' AS VARCHAR))");
        assertThat(query(
                "SELECT element_at(map_value, 'earlier'), timezone(element_at(map_value, 'earlier')), " +
                        "element_at(map_value, 'later'), timezone(element_at(map_value, 'later')) " +
                        "FROM remote.default.test_nested_tstz_dst_fold"))
                .matches("VALUES (" + earlier + ", CAST('Europe/Warsaw' AS VARCHAR), " + later + ", CAST('Europe/Warsaw' AS VARCHAR))");
        assertThat(query(
                "SELECT row_value.earlier, timezone(row_value.earlier), row_value.later, timezone(row_value.later) " +
                        "FROM remote.default.test_nested_tstz_dst_fold"))
                .matches("VALUES (" + earlier + ", CAST('Europe/Warsaw' AS VARCHAR), " + later + ", CAST('Europe/Warsaw' AS VARCHAR))");
    }

    @Test
    void testTimestampWithTimeZoneHistoricalRegionOffset()
    {
        String value = "CAST(at_timezone(TIMESTAMP '1890-01-01 00:00:00.000 UTC', 'Europe/Paris') AS TIMESTAMP(3) WITH TIME ZONE)";

        assertThat(query("SELECT value, timezone(value) FROM remote.default.test_tstz_historical_zone"))
                .matches("VALUES (" + value + ", CAST('Europe/Paris' AS VARCHAR))");
        assertThat(query("SELECT value FROM remote.default.test_tstz_historical_zone WHERE value = " + value))
                .isFullyPushedDown()
                .matches("VALUES " + value);
    }

    // =========================================================================
    // Scalar types via remote memory catalog
    // =========================================================================

    @Test
    void testDateRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_date ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("1999-12-31");
        assertThat(result.getMaterializedRows().get(1).getField(0).toString()).isEqualTo("2024-01-15");
    }

    @Test
    void testHistoricalDateRoundTrip()
    {
        Session delegationOff = Session.builder(getSession())
                .setCatalogSessionProperty("remote", "remote_delegation_enabled", "false")
                .build();

        assertThat(query(
                delegationOff,
                "SELECT id, CAST(x AS VARCHAR) FROM remote.default.test_historical_date ORDER BY id"))
                .ordered()
                .matches("VALUES " +
                        "(1, CAST('-0001-01-01' AS VARCHAR)), " +
                        "(2, CAST('0000-01-01' AS VARCHAR)), " +
                        "(3, CAST('1582-10-10' AS VARCHAR)), " +
                        "(4, CAST('10000-01-01' AS VARCHAR))");
        assertThat(query(
                delegationOff,
                "SELECT id FROM remote.default.test_historical_date WHERE x = DATE '-0001-01-01'"))
                .matches("VALUES 1");
    }

    @Test
    void testTimestamp6Precision()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_timestamp6").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456");
    }

    @Test
    void testUuid()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_uuid");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString())
                .isEqualToIgnoringCase("12345678-1234-1234-1234-123456789abc");
    }

    // =========================================================================
    // Complex types with temporal/decimal inner types
    // =========================================================================

    @Test
    void testArrayOfDate()
    {
        // array(date) — temporal type inside complex type
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_date");
        assertThat(result.getRowCount()).isEqualTo(1);
        // Unnest and verify individual dates
        MaterializedResult unnested = computeActual(
                "SELECT e FROM remote.default.test_array_date CROSS JOIN UNNEST(x) AS t(e) ORDER BY e");
        assertThat(unnested.getRowCount()).isEqualTo(2);
        assertThat(unnested.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("2024-01-01");
        assertThat(unnested.getMaterializedRows().get(1).getField(0).toString()).isEqualTo("2024-06-15");
    }

    @Test
    void testArrayOfHistoricalDate()
    {
        Session delegationOff = Session.builder(getSession())
                .setCatalogSessionProperty("remote", "remote_delegation_enabled", "false")
                .build();

        assertThat(query(
                delegationOff,
                "SELECT CAST(x[1] AS VARCHAR), CAST(x[2] AS VARCHAR), CAST(x[3] AS VARCHAR), CAST(x[4] AS VARCHAR) FROM remote.default.test_array_historical_date"))
                .matches("VALUES (" +
                        "CAST('-0001-01-01' AS VARCHAR), " +
                        "CAST('0000-01-01' AS VARCHAR), " +
                        "CAST('1582-10-10' AS VARCHAR), " +
                        "CAST('10000-01-01' AS VARCHAR))");
    }

    @Test
    void testArrayOfTimePreservesFractionalSeconds()
    {
        Session delegationOff = Session.builder(getSession())
                .setCatalogSessionProperty("remote", "remote_delegation_enabled", "false")
                .build();

        assertThat(query(
                delegationOff,
                "SELECT CAST(p0[1] AS VARCHAR), CAST(p3[1] AS VARCHAR), CAST(p6[1] AS VARCHAR), CAST(p9[1] AS VARCHAR) FROM remote.default.test_array_time_precision"))
                .matches("VALUES (" +
                        "CAST('10:30:45' AS VARCHAR), " +
                        "CAST('10:30:45.123' AS VARCHAR), " +
                        "CAST('10:30:45.123456' AS VARCHAR), " +
                        "CAST('10:30:45.123456789' AS VARCHAR))");
    }

    @Test
    void testMapWithDecimalValues()
    {
        // map(varchar, decimal) — decimal type inside complex type
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'price') FROM remote.default.test_map_decimal");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("99.99");
    }

    // =========================================================================
    // Type identity verification via DESCRIBE
    // =========================================================================

    @Test
    void testDescribeScalarTypes()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_date");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("date");
    }

    @Test
    void testDescribeUuid()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_uuid");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("uuid");
    }

    @Test
    void testDescribeTimestamp6()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_timestamp6");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("timestamp(6)");
    }

    @Test
    void testDescribeArrayOfDate()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_array_date");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("array");
        assertThat(typeStr).contains("date");
    }

    // =========================================================================
    // Existing complex type round-trip verification
    // =========================================================================

    @Test
    void testArrayVarcharRoundTrip()
    {
        MaterializedResult result = computeActual(
                "SELECT e FROM remote.default.test_array_varchar CROSS JOIN UNNEST(x) AS t(e) ORDER BY e");
        assertThat(result.getRowCount()).isEqualTo(4);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("a");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("b");
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("c");
        assertThat(result.getMaterializedRows().get(3).getField(0)).isEqualTo("d");
    }

    @Test
    void testMapRoundTrip()
    {
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'k1') FROM remote.default.test_map_vv WHERE element_at(x, 'k1') IS NOT NULL");
        assertThat(result.getOnlyValue()).isEqualTo("v1");
    }

    @Test
    void testRowRoundTrip()
    {
        MaterializedResult result = computeActual(
                "SELECT x.name, x.age FROM remote.default.test_row ORDER BY x.name");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Bob");
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(25);
    }

    @Test
    void testNestedComplexTypeRoundTrip()
    {
        // row(svc varchar, evts array(map(varchar, varchar)))
        MaterializedResult result = computeActual(
                """
                SELECT
                    d.svc,
                    element_at(evt, 'type') AS evt_type
                FROM remote.default.test_nested_row
                CROSS JOIN UNNEST(d.evts) AS t(evt)
                ORDER BY evt_type
                """);
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("article");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("click");
    }

    // =========================================================================
    // Nested complex types with temporal/UUID inner types
    // =========================================================================

    @Test
    void testArrayOfTimestamp()
    {
        MaterializedResult unnested = computeActual(
                """
                SELECT CAST(e AS VARCHAR)
                FROM remote.default.test_array_timestamp
                CROSS JOIN UNNEST(x) AS t(e)
                ORDER BY 1
                """);
        assertThat(unnested.getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        "2024-01-15 10:30:45.123456",
                        "2024-06-15 23:59:59.000000");
    }

    @Test
    void testArrayOfTimestampWithTimeZone()
    {
        MaterializedResult unnested = computeActual(
                """
                SELECT CAST(e AS VARCHAR)
                FROM remote.default.test_array_tstz
                CROSS JOIN UNNEST(x) AS t(e)
                ORDER BY 1
                """);
        assertThat(unnested.getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        "2024-01-15 10:30:45.123 UTC",
                        "2024-06-15 12:00:00.000 +09:00");
    }

    @Test
    void testMapWithUuidValues()
    {
        // map(varchar, uuid)
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'id1') FROM remote.default.test_map_uuid");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString())
                .isEqualToIgnoringCase("12345678-1234-1234-1234-123456789abc");
    }

    @Test
    void testUuidNullRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_uuid_null ORDER BY id");
        assertThat(result.getMaterializedRows().get(0).getField(0))
                .isEqualTo("12345678-1234-1234-1234-123456789abc");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    @Test
    void testRowInsideRow()
    {
        // row(inner_val row(x integer, y varchar), label varchar)
        MaterializedResult result = computeActual(
                "SELECT d.inner_val.x, d.inner_val.y, d.label FROM remote.default.test_row_in_row");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("hello");
        assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo("outer");
    }

    @Test
    void testQuotedRowFieldNames()
    {
        // row("my field" varchar, "my count" integer) — quoted field names
        // The JDBC driver may strip quotes from type metadata. The parser
        // uses right-to-left type suffix matching to recover field names.
        MaterializedResult result = computeActual(
                "SELECT d FROM remote.default.test_quoted_row_field");
        assertThat(result.getRowCount()).isEqualTo(1);
        String rowStr = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(rowStr).contains("value");
        assertThat(rowStr).contains("42");
    }

    // =========================================================================
    // DESCRIBE verification for new complex types
    // =========================================================================

    @Test
    void testDescribeArrayOfTimestamp()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_array_timestamp");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("array");
        assertThat(typeStr).contains("timestamp");
    }

    @Test
    void testDescribeRowInRow()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_row_in_row");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("row");
    }

    @Test
    void testDecimalRoundTrip()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_decimal ORDER BY x").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("123.45", "999.99");
    }

    @Test
    void testNumberNativeType()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_number");
        assertThat(result.getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("0.1", "3.1415", "20050910133100123");
    }

    @Test
    void testDescribeNumber()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_number");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("number");
    }

    @Test
    void testNumberPredicatePushdown()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_number WHERE x = NUMBER '0.1'");
        assertThat(result.getOnlyValue()).isEqualTo("0.1");
    }

    @Test
    void testNumberDelegatedExpressionWithConstant()
    {
        String sql = "SELECT CAST(x + NUMBER '1' AS VARCHAR) FROM remote.default.test_number WHERE x = NUMBER '0.1'";
        MaterializedResult result = computeActual(sql);
        assertThat(result.getOnlyValue()).isEqualTo("1.1");

        // The predicate and the arithmetic projection are delegated remotely: the scan
        // collapses into a query relation and no local filter remains above it
        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("remote:Query[");
        assertThat(explain).doesNotContain("ScanFilterProject");
    }

    @Test
    void testNumberSpecialValues()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_number_special");
        assertThat(result.getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("NaN", "+Infinity", "-Infinity");
    }

    @Test
    void testBooleanRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_boolean ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(false);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(true);
    }

    // =========================================================================
    // Additional type coverage: TIME, CHAR, VARBINARY, Long Decimal/Timestamp
    // =========================================================================

    @Test
    void testTimeRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_time ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("10:30:45");
        assertThat(result.getMaterializedRows().get(1).getField(0).toString()).isEqualTo("23:59:59");
    }

    @Test
    void testTimePrecision6()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_time6");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("10:30:45.123456");
    }

    @Test
    void testTimePredicatePushdown()
    {
        // Regression: the time column mapping used to bind pushed-down predicate values
        // as "TIME '...'" strings, which the remote rejects as time = varchar
        assertThat(query("SELECT x FROM remote.default.test_time WHERE x = TIME '10:30:45'"))
                .isFullyPushedDown();
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_time WHERE x = TIME '10:30:45'");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("10:30:45");

        MaterializedResult range = computeActual("SELECT x FROM remote.default.test_time WHERE x > TIME '12:00:00'");
        assertThat(range.getRowCount()).isEqualTo(1);
        assertThat(range.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("23:59:59");

        MaterializedResult precise = computeActual("SELECT x FROM remote.default.test_time6 WHERE x = TIME '10:30:45.123456'");
        assertThat(precise.getRowCount()).isEqualTo(1);
        assertThat(precise.getMaterializedRows().get(0).getField(0).toString()).isEqualTo("10:30:45.123456");
    }

    @Test
    void testTimeConstantParameterInDelegatedProjection()
    {
        // Regression: TIME constants in delegated expressions bind through toWriteMapping,
        // which used the standard write function rejected by the Trino JDBC driver
        // ("Unsupported object type: java.time.LocalTime"). The OR keeps the expression
        // off the tuple-domain path so the constant stays a renderer-bound parameter.
        MaterializedResult result = computeActual(
                "SELECT (x = TIME '10:30:45' OR x IS NULL) FROM remote.default.test_time ORDER BY 1");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(false);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(true);
    }

    @Test
    void testTimeWithTimeZoneTransport()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_timetz3");
        assertThat(result.getOnlyValue()).isEqualTo("10:30:45.123+09:00");
    }

    @Test
    void testCharType()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR), length(x) FROM remote.default.test_char");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("abc");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(10L);
    }

    @Test
    void testArrayCharType()
    {
        MaterializedResult result = computeActual(
                """
                SELECT CAST(e AS VARCHAR), length(e)
                FROM remote.default.test_array_char
                CROSS JOIN UNNEST(x) AS t(e)
                """);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("abc");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(10L);
    }

    @Test
    void testVarbinaryRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_varbinary ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    void testTimestampWithTimeZoneTopLevel()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_tstz").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123 UTC");
    }

    @Test
    void testLongDecimal()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_long_decimal").getOnlyValue())
                .isEqualTo("12345678901234567890.12345");
    }

    @Test
    void testLongTimestamp()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_timestamp9").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456789");
    }

    @Test
    void testLongTimestampWithTimeZone()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_tstz6").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456 UTC");
    }

    @Test
    void testTimestampWithTimeZonePrecision0()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_tstz0").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45 UTC");
    }

    @Test
    void testTimestampWithTimeZonePrecision1()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_tstz1").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.1 UTC");
    }

    @Test
    void testTimestampWithTimeZonePrecision2()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_tstz2").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.12 UTC");
    }

    @Test
    void testTimestampWithTimeZonePrecision9()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_tstz9").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456789 UTC");
    }

    @Test
    void testDescribeTime()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_time6");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("time");
    }

    @Test
    void testDescribeVarbinary()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_varbinary");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("varbinary");
    }

    @Test
    void testDescribeLongDecimal()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_long_decimal");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("decimal(38,5)");
    }

    // =========================================================================
    // VARCHAR transport fallback
    // =========================================================================

    @Test
    void testTimestampWithTimeZonePrecision12Transport()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_tstz12");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("timestamp(12) with time zone");
        });
        assertThat(computeActual("SELECT CAST(unsupported_col AS VARCHAR) FROM remote.default.test_tstz12").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456789012 UTC");
    }

    @Test
    void testPlainTimestampPrecision12Transport()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_ts12");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("timestamp(12)");
        });
        assertThat(computeActual("SELECT CAST(unsupported_col AS VARCHAR) FROM remote.default.test_ts12").getOnlyValue())
                .isEqualTo("2024-01-15 10:30:45.123456789012");
    }

    @Test
    void testIntervalDayToSecondTransport()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_interval_day_to_second").getOnlyValue())
                .isEqualTo("2 03:04:05.678");
    }

    @Test
    void testIntervalYearToMonthTransport()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_interval_year_to_month").getOnlyValue())
                .isEqualTo("1-6");
    }

    @Test
    void testTimePrecision12Transport()
    {
        assertThat(computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_time12").getOnlyValue())
                .isEqualTo("10:30:45.123456789012");
    }

    @Test
    void testHyperLogLogVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_hyperloglog").getOnlyValue())
                .isEqualTo("HyperLogLog");
        assertThat(computeActual("SELECT cardinality(CAST(CAST(x AS VARBINARY) AS HyperLogLog)) FROM remote.default.test_hyperloglog").getOnlyValue())
                .isEqualTo(5L);
    }

    @Test
    void testQDigestVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_qdigest").getOnlyValue())
                .isEqualTo("qdigest(bigint)");
        assertThat(computeActual("SELECT value_at_quantile(CAST(CAST(x AS VARBINARY) AS qdigest(bigint)), 0.5) FROM remote.default.test_qdigest").getOnlyValue())
                .isEqualTo(3L);
    }

    @Test
    void testTDigestVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_tdigest").getOnlyValue())
                .isEqualTo("tdigest");
        assertThat(computeActual("SELECT value_at_quantile(CAST(CAST(x AS VARBINARY) AS tdigest), 0.5) FROM remote.default.test_tdigest").getOnlyValue())
                .isEqualTo(3.0);
    }

    @Test
    void testSetDigestVarbinaryTransport()
    {
        assertThat(computeActual("SELECT typeof(x) FROM remote.default.test_setdigest").getOnlyValue().toString())
                .isEqualToIgnoringCase("setdigest");
        assertThat(computeActual("SELECT cardinality(CAST(CAST(x AS VARBINARY) AS setdigest)) FROM remote.default.test_setdigest").getOnlyValue())
                .isEqualTo(5L);
    }

    @Test
    void testNestedArrayWithUnsupportedElementTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_array");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("array(time(3) with time zone)");
        });
        assertThat(computeActual(
                """
                SELECT CAST(e AS VARCHAR)
                FROM remote.default.test_nested_unsupported_array
                CROSS JOIN UNNEST(unsupported_col) AS t(e)
                """).getOnlyValue()).isEqualTo("10:30:45.123+09:00");
    }

    @Test
    void testNestedMapWithUnsupportedValueTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_map");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).contains("map");
            assertThat(row.getField(1).toString()).contains("interval day to second");
        });
        assertThat(computeActual(
                """
                SELECT CAST(element_at(unsupported_col, 'duration') AS VARCHAR)
                FROM remote.default.test_nested_unsupported_map
                """).getOnlyValue()).isEqualTo("1 00:00:00.000");
    }

    @Test
    void testNestedMapWithUnsupportedIntegerKeyTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_map_int_key");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).contains("map");
            assertThat(row.getField(1).toString()).contains("integer");
            assertThat(row.getField(1).toString()).contains("interval day to second");
        });
        assertThat(computeActual(
                """
                SELECT CAST(element_at(unsupported_col, 2) AS VARCHAR)
                FROM remote.default.test_nested_unsupported_map_int_key
                """).getOnlyValue()).isEqualTo("2 00:00:00.000");
    }

    @Test
    void testNestedMapWithUnsupportedRowKeyTransport()
    {
        assertThat(computeActual(
                """
                SELECT CAST(v AS VARCHAR)
                FROM remote.default.test_nested_unsupported_map_row_key
                CROSS JOIN UNNEST(unsupported_col) AS t(k, v)
                WHERE k.x = 2
                """).getOnlyValue()).isEqualTo("2 00:00:00.000");
    }

    @Test
    void testNestedRowWithUnsupportedFieldTransport()
    {
        MaterializedResult describe = computeActual("DESCRIBE remote.default.test_nested_unsupported_row");
        assertThat(describe.getRowCount()).isEqualTo(2);
        assertThat(describe.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("unsupported_col");
            assertThat(row.getField(1).toString()).isEqualTo("row(\"x\" interval day to second)");
        });
        assertThat(computeActual(
                """
                SELECT CAST(unsupported_col.x AS VARCHAR)
                FROM remote.default.test_nested_unsupported_row
                """).getOnlyValue()).isEqualTo("1 00:00:00.000");
    }

    @Test
    void testOpaqueUnsupportedTypeConvertsToVarchar()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_unsupported_color");
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo("x");
            assertThat(row.getField(1)).isEqualTo("varchar");
        });
    }

    @Test
    void testTransportBackedTypesDoNotFallBackToVarchar()
    {
        assertDescribeColumnType("test_hyperloglog", "x", "HyperLogLog");
        assertDescribeColumnType("test_qdigest", "x", "qdigest(bigint)");
        assertDescribeColumnType("test_tdigest", "x", "tdigest");
        assertDescribeColumnType("test_setdigest", "x", "setdigest");
        assertDescribeColumnType("test_nested_unsupported_array", "unsupported_col", "array(time(3) with time zone)");
        assertDescribeColumnType("test_nested_unsupported_map", "unsupported_col", "map(varchar(8), interval day to second)");
        assertDescribeColumnType("test_nested_unsupported_row", "unsupported_col", "row(\"x\" interval day to second)");
    }

    private void assertDescribeColumnType(String tableName, String columnName, String expectedType)
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default." + tableName);
        assertThat(result.getMaterializedRows()).anySatisfy(row -> {
            assertThat(row.getField(0)).isEqualTo(columnName);
            assertThat(row.getField(1).toString()).isEqualToIgnoringCase(expectedType);
        });
    }

    // =========================================================================
    // JSON and IPADDRESS native type mapping
    // =========================================================================

    @Test
    void testJsonNativeType()
    {
        // JSON should be mapped to native json type, not varchar
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_json");
        assertThat(result.getRowCount()).isEqualTo(1);
        String value = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(value).contains("key");
        assertThat(value).contains("value");
    }

    @Test
    void testDescribeJson()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_json");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("json");
    }

    @Test
    void testJsonNullRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT json_format(x) FROM remote.default.test_json_null ORDER BY id");
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("\"key\"");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    @Test
    void testIpAddressNativeType()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_ipaddress");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("192.168.1.1");
    }

    @Test
    void testDescribeIpAddress()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_ipaddress");
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).isEqualTo("ipaddress");
    }

    @Test
    void testIpAddressNullRoundTrip()
    {
        MaterializedResult result = computeActual("SELECT CAST(x AS VARCHAR) FROM remote.default.test_ipaddress_null ORDER BY id");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("192.168.1.1");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    // =========================================================================
    // Nested JSON and IPADDRESS inside complex types
    // =========================================================================

    @Test
    void testArrayOfJson()
    {
        // JSON is not orderable, so no ORDER BY — verify both elements are present
        MaterializedResult unnested = computeActual(
                "SELECT e FROM remote.default.test_array_json CROSS JOIN UNNEST(x) AS t(e)");
        assertThat(unnested.getRowCount()).isEqualTo(2);
        String all = unnested.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(joining(","));
        assertThat(all).contains("\"a\"");
        assertThat(all).contains("\"b\"");
    }

    @Test
    void testMapOfIpAddress()
    {
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'server1') FROM remote.default.test_map_ipaddress");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0).toString()).contains("192.168.1.1");
    }

    @Test
    void testRowWithJson()
    {
        MaterializedResult result = computeActual(
                "SELECT x.label, x.data FROM remote.default.test_row_json");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("test");
        assertThat(result.getMaterializedRows().get(0).getField(1).toString()).contains("key");
    }

    // Connector integration coverage

    // =========================================================================
    // 1. Basic connectivity
    // =========================================================================

    @Test
    void testShowCatalogs()
    {
        MaterializedResult result = computeActual("SHOW CATALOGS");
        assertThat(result.getOnlyColumnAsSet()).contains("remote", "tpch", "system");
    }

    @Test
    void testIntegrationShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM remote");
        assertThat(result.getOnlyColumnAsSet()).contains("default", "alt", "information_schema");
    }

    @Test
    void testIntegrationShowTables()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM remote.default");
        assertThat(result.getOnlyColumnAsSet()).contains("nation", "region", "orders");
    }

    @Test
    void testShowTablesInAdditionalSchema()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM remote.alt");
        assertThat(result.getOnlyColumnAsSet()).contains("access_log");
    }

    @Test
    void testConfiguredCatalogScopeDoesNotExposeOtherRemoteCatalogAsSchema()
    {
        assertThatThrownBy(() -> computeActual("SHOW TABLES FROM remote.tiny"))
                .hasMessageContaining("Schema 'tiny' does not exist");
    }

    // =========================================================================
    // 2. Scalar type read tests (via remote tpch)
    // =========================================================================

    @Test
    void testBigint()
    {
        // remote memory.default.orders.orderkey is BIGINT (copied from tpch.tiny)
        MaterializedResult result = computeActual("SELECT orderkey FROM remote.default.orders ORDER BY orderkey LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isInstanceOf(Long.class);
    }

    @Test
    void testVarchar()
    {
        MaterializedResult remote = computeActual("SELECT name FROM remote.default.nation ORDER BY name LIMIT 5");
        MaterializedResult local = computeActual("SELECT name FROM tpch.tiny.nation ORDER BY name LIMIT 5");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testDouble()
    {
        MaterializedResult result = computeActual("SELECT totalprice FROM remote.default.orders ORDER BY orderkey LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isInstanceOf(Double.class);
    }

    @Test
    void testInteger()
    {
        MaterializedResult remote = computeActual("SELECT regionkey FROM remote.default.nation ORDER BY nationkey LIMIT 5");
        MaterializedResult local = computeActual("SELECT regionkey FROM tpch.tiny.nation ORDER BY nationkey LIMIT 5");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testDate()
    {
        MaterializedResult result = computeActual("SELECT orderdate FROM remote.default.orders ORDER BY orderkey LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        // Date values should not be null
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNotNull();
    }

    // =========================================================================
    // 3. Aggregation & filtering
    // =========================================================================

    @Test
    void testCount()
    {
        MaterializedResult remote = computeActual("SELECT COUNT(*) FROM remote.default.nation");
        MaterializedResult local = computeActual("SELECT COUNT(*) FROM tpch.tiny.nation");
        assertThat(remote.getOnlyValue()).isEqualTo(local.getOnlyValue());
    }

    @Test
    void testFilterPushdown()
    {
        MaterializedResult remote = computeActual("SELECT name FROM remote.default.nation WHERE regionkey = 1 ORDER BY name");
        MaterializedResult local = computeActual("SELECT name FROM tpch.tiny.nation WHERE regionkey = 1 ORDER BY name");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testGroupBy()
    {
        MaterializedResult remote = computeActual("SELECT regionkey, COUNT(*) FROM remote.default.nation GROUP BY regionkey ORDER BY regionkey");
        MaterializedResult local = computeActual("SELECT regionkey, COUNT(*) FROM tpch.tiny.nation GROUP BY regionkey ORDER BY regionkey");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testOrderByLimit()
    {
        MaterializedResult remote = computeActual("SELECT nationkey, name FROM remote.default.nation ORDER BY nationkey DESC LIMIT 3");
        MaterializedResult local = computeActual("SELECT nationkey, name FROM tpch.tiny.nation ORDER BY nationkey DESC LIMIT 3");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    // =========================================================================
    // 4. Cross-catalog JOIN (the core use case)
    // =========================================================================

    @Test
    void testCrossCatalogJoin()
    {
        MaterializedResult result = computeActual(
                """
                SELECT r.name AS remote_name, l.name AS local_name
                FROM remote.default.region r
                JOIN tpch.tiny.region l ON r.regionkey = l.regionkey
                ORDER BY r.regionkey
                """);
        assertThat(result.getRowCount()).isEqualTo(5);
        // Names should be identical since both are tpch
        for (var row : result.getMaterializedRows()) {
            assertThat(row.getField(0)).isEqualTo(row.getField(1));
        }
    }

    @Test
    void testCrossCatalogJoinWithFilter()
    {
        MaterializedResult result = computeActual(
                """
                SELECT n.name
                FROM remote.default.nation n
                JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                WHERE r.name = 'EUROPE'
                ORDER BY n.name
                """);
        assertThat(result.getRowCount()).isEqualTo(5); // 5 European nations in tpch
    }

    // =========================================================================
    // 5. Complex types: ARRAY
    // =========================================================================

    @Test
    void testArrayVarchar()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_varchar");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    void testArrayInteger()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_int");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    void testArrayWithNulls()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_array_nulls");
        assertThat(result.getRowCount()).isEqualTo(2);
        // Second row should be NULL (entire array is null)
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    @Test
    void testArrayUnnest()
    {
        MaterializedResult result = computeActual(
                "SELECT e FROM remote.default.test_array_varchar CROSS JOIN UNNEST(x) AS t(e) ORDER BY e");
        // ['a','b','c'] + ['d'] + [] = 4 elements
        assertThat(result.getRowCount()).isEqualTo(4);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("a");
    }

    // =========================================================================
    // 6. Complex types: MAP
    // =========================================================================

    @Test
    void testMapVarcharVarchar()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_map_vv");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    void testMapSubscript()
    {
        MaterializedResult result = computeActual(
                "SELECT element_at(x, 'k1') FROM remote.default.test_map_vv WHERE element_at(x, 'k1') IS NOT NULL");
        assertThat(result.getOnlyValue()).isEqualTo("v1");
    }

    @Test
    void testMapWithNullValues()
    {
        MaterializedResult result = computeActual("SELECT x['b'] FROM remote.default.test_map_nulls");
        assertThat(result.getOnlyValue()).isNull();
    }

    // =========================================================================
    // 7. Complex types: ROW
    // =========================================================================

    @Test
    void testRow()
    {
        MaterializedResult result = computeActual(
                "SELECT x.name, x.age FROM remote.default.test_row ORDER BY x.name");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30);
    }

    @Test
    void testRowWithNull()
    {
        // First row: ROW with NULL field inside, Second row: entire ROW is NULL
        MaterializedResult result = computeActual("SELECT x.name FROM remote.default.test_row_null ORDER BY x.name NULLS LAST");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
        // Second: NULL row → NULL name
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    @Test
    void testJsonTransportPreservesNullRow()
    {
        Session delegationOff = Session.builder(getSession())
                .setCatalogSessionProperty("remote", "remote_delegation_enabled", "false")
                .build();

        assertThat(query(delegationOff, "SELECT id FROM remote.default.test_row_temporal_null WHERE x IS NULL"))
                .matches("VALUES 1");

        MaterializedResult rows = computeActual(
                delegationOff,
                "SELECT id, x FROM remote.default.test_row_temporal_null ORDER BY id");
        assertThat(rows.getMaterializedRows().get(0).getField(1)).isNull();
        assertThat(rows.getMaterializedRows().get(1).getField(1)).isNotNull();
        assertThat(rows.getMaterializedRows().get(2).getField(1)).isNotNull();

        assertThat(query(
                delegationOff,
                "SELECT x[1] IS NULL, x[2] IS NULL FROM remote.default.test_array_row_temporal_null"))
                .matches("VALUES (true, false)");
    }

    // =========================================================================
    // 8. Deeply nested complex types
    // =========================================================================

    @Test
    void testArrayOfMap()
    {
        // array(map(varchar, varchar)) — common event log pattern
        MaterializedResult result = computeActual(
                "SELECT element_at(evt, 'type') FROM remote.default.test_array_of_map CROSS JOIN UNNEST(evts) AS t(evt) ORDER BY 1");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("click");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("pageview");
    }

    @Test
    void testRowContainingArrayOfMap()
    {
        // row(svc varchar, evts array(map(varchar, varchar))) — nested complex type
        MaterializedResult result = computeActual(
                """
                SELECT
                    d.svc,
                    element_at(evt, 'type') AS evt_type
                FROM remote.default.test_nested_row
                CROSS JOIN UNNEST(d.evts) AS t(evt)
                ORDER BY evt_type
                """);
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("article");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("click");
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("pageview");
    }

    @Test
    void testCrossJoinWithNestedUnnest()
    {
        // Full scenario: UNNEST complex type from remote + JOIN with local tpch
        MaterializedResult result = computeActual(
                """
                SELECT
                    element_at(evt, 'post_id') AS post_id,
                    n.name AS nation_name
                FROM remote.default.test_array_of_map
                CROSS JOIN UNNEST(evts) AS t(evt)
                JOIN tpch.tiny.nation n ON CAST(element_at(evt, 'nation_key') AS BIGINT) = n.nationkey
                ORDER BY post_id
                """);
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("p1");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("BRAZIL");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("p2");
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("CANADA");
    }

    // =========================================================================
    // 9. Boolean, Decimal
    // =========================================================================

    @Test
    void testBoolean()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_boolean ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(false);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(true);
    }

    @Test
    void testDecimal()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_decimal ORDER BY x");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    // =========================================================================
    // 10. NULL handling
    // =========================================================================

    @Test
    void testNullScalar()
    {
        MaterializedResult result = computeActual("SELECT x FROM remote.default.test_null_varchar ORDER BY x NULLS LAST");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("hello");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isNull();
    }

    // =========================================================================
    // 11. Query passthrough
    // =========================================================================

    @Test
    void testQueryPassthrough()
    {
        MaterializedResult result = computeActual(
                """
                SELECT * FROM TABLE(remote.system.query(
                    query => 'SELECT nationkey, name FROM memory.default.nation ORDER BY nationkey LIMIT 3'
                ))
                """);
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    void testQueryPassthroughWithAnonymousOutput()
    {
        MaterializedResult result = computeActual(
                """
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT count(*) FROM memory.default.nation'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo(25L);
    }

    @Test
    void testQueryPassthroughCanUseFullyQualifiedRemoteSql()
    {
        MaterializedResult result = computeActual(
                """
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT name FROM memory.default.nation WHERE nationkey = 0'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo("ALGERIA");
    }

    @Test
    void testQueryPassthroughPreservesRemoteError()
    {
        // A query that fails remote preparation (unknown table) must surface the
        // remote error instead of a generic connector failure
        assertThatThrownBy(() -> computeActual(
                """
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT x FROM memory.default.no_such_table_anywhere'
                ))
                """))
                .hasMessageContaining("no_such_table_anywhere");
    }

    @Test
    void testQueryPassthroughCanReferenceOtherRemoteCatalog()
    {
        // Passthrough SQL is an explicit escape hatch from the 1:1 catalog mapping:
        // it may reference any remote catalog the remote credentials can access
        MaterializedResult result = computeActual(
                """
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT name FROM tpch.tiny.nation WHERE nationkey = 0'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo("ALGERIA");
    }

    @Test
    void testQueryPassthroughWithCte()
    {
        MaterializedResult result = computeActual(
                """
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'WITH ranked AS (
                        SELECT nationkey, name
                        FROM memory.default.nation
                        WHERE regionkey = 1
                    )
                    SELECT nationkey, name FROM ranked ORDER BY nationkey LIMIT 2'
                ))
                """);
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("ARGENTINA");
    }

    @Test
    void testQueryPassthroughWithWindowFunction()
    {
        MaterializedResult result = computeActual(
                """
                SELECT *
                FROM TABLE(remote.system.query(
                    query => 'SELECT name, row_number() OVER (ORDER BY nationkey) AS rn
                        FROM memory.default.nation
                        ORDER BY nationkey
                        LIMIT 3'
                ))
                """);
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(1L);
    }

    @Test
    void testQueryPassthroughNumberResult()
    {
        MaterializedResult result = computeActual(
                """
                SELECT CAST(x AS VARCHAR)
                FROM TABLE(remote.system.query(
                    query => 'SELECT NUMBER ''0.1'' AS x'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo("0.1");
    }

    @Test
    void testQueryPassthroughUnsupportedStructuralResult()
    {
        MaterializedResult result = computeActual(
                """
                SELECT CAST(element_at(m, 2) AS VARCHAR)
                FROM TABLE(remote.system.query(
                    query => 'SELECT MAP(ARRAY[1, 2], ARRAY[INTERVAL ''1'' DAY, INTERVAL ''2'' DAY]) AS m'
                ))
                """);
        assertThat(result.getOnlyValue()).isEqualTo("2 00:00:00.000");
    }

    @Test
    void testQueryPassthroughIntervalResults()
    {
        assertThat(query(
                """
                SELECT day_interval, month_interval
                FROM TABLE(remote.system.query(
                    query => 'SELECT INTERVAL ''1'' DAY AS day_interval, INTERVAL ''14'' MONTH AS month_interval'
                ))
                """))
                .matches("VALUES (INTERVAL '1' DAY, INTERVAL '14' MONTH)");

        assertThat(query(
                """
                SELECT day_interval, month_interval
                FROM TABLE(remote.system.query(
                    query => 'SELECT CAST(NULL AS INTERVAL DAY TO SECOND) AS day_interval, CAST(NULL AS INTERVAL YEAR TO MONTH) AS month_interval'
                ))
                """))
                .matches("VALUES (CAST(NULL AS INTERVAL DAY TO SECOND), CAST(NULL AS INTERVAL YEAR TO MONTH))");
    }

    @Test
    void testQueryPassthroughTrailingSemicolon()
    {
        assertThat(query("SELECT * FROM TABLE(remote.system.query(query => 'SELECT 1 AS value;'))"))
                .matches("VALUES 1");
    }

    @Test
    void testQueryPassthroughExpandedPositiveTimestampYear()
    {
        assertThat(query(
                """
                SELECT CAST(value AS VARCHAR)
                FROM TABLE(remote.system.query(
                    query => 'SELECT CAST(TIMESTAMP ''+10000-01-01 00:00:00.123456789012'' AS TIMESTAMP(12)) AS value'
                ))
                """))
                .matches("VALUES CAST('+10000-01-01 00:00:00.123456789012' AS VARCHAR)");
    }

    // =========================================================================
    // 12. DESCRIBE
    // =========================================================================

    @Test
    void testIntegrationDescribeTable()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.nation");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    @Test
    void testDescribeComplexTable()
    {
        MaterializedResult result = computeActual("DESCRIBE remote.default.test_nested_row");
        assertThat(result.getRowCount()).isGreaterThan(0);
        // Should show 'd' column with row(...) type
        String typeStr = result.getMaterializedRows().get(0).getField(1).toString();
        assertThat(typeStr).contains("row");
        assertThat(typeStr).contains("array");
        assertThat(typeStr).contains("map");
    }

    @Test
    void testShowStats()
    {
        MaterializedResult result = computeActual("SHOW STATS FOR remote.default.nation");
        assertThat(result.getRowCount()).isGreaterThan(0);
        assertThat(result.getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst())
                .isPresent();
    }

    @Test
    void testStatisticsDisabledConfig()
    {
        // Baseline: remote statistics feed optimizer estimates when enabled (default)
        String enabled = computeActual("EXPLAIN SELECT nationkey FROM remote.default.nation").getOnlyValue().toString();
        assertThat(enabled).contains("rows: 25");

        // Regression: statistics.enabled was bound but ignored, so remote SHOW STATS
        // statistics flowed into estimates even when disabled
        String disabled = computeActual("EXPLAIN SELECT nationkey FROM remote_stats_disabled.default.nation").getOnlyValue().toString();
        assertThat(disabled).doesNotContain("rows: 25");
    }

    // =========================================================================
    // 13. Pushdown verification
    //
    // These are smoke tests for remote SQL generation and end-to-end correctness.
    // Planner-level pushdown assertions live in TestTrinoConnectorTest.
    // =========================================================================

    @Test
    void testIntegrationLimitPushdown()
    {
        MaterializedResult result = computeActual("SELECT nationkey FROM remote.default.nation LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    void testIntegrationTopNPushdown()
    {
        // Verify ORDER BY + LIMIT produces correct, ordered results matching local tpch
        MaterializedResult remote = computeActual(
                "SELECT nationkey FROM remote.default.nation ORDER BY nationkey LIMIT 3");
        MaterializedResult local = computeActual(
                "SELECT nationkey FROM tpch.tiny.nation ORDER BY nationkey LIMIT 3");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testIntegrationAggregationPushdown()
    {
        // Verify aggregation results match local tpch exactly
        MaterializedResult remoteCount = computeActual(
                "SELECT COUNT(*) FROM remote.default.nation");
        MaterializedResult localCount = computeActual(
                "SELECT COUNT(*) FROM tpch.tiny.nation");
        assertThat(remoteCount.getOnlyValue()).isEqualTo(localCount.getOnlyValue());

        MaterializedResult remoteSum = computeActual(
                "SELECT SUM(regionkey) FROM remote.default.nation");
        MaterializedResult localSum = computeActual(
                "SELECT SUM(regionkey) FROM tpch.tiny.nation");
        assertThat(remoteSum.getOnlyValue()).isEqualTo(localSum.getOnlyValue());

        // GROUP BY + COUNT: compare full result sets
        MaterializedResult remoteGrouped = computeActual(
                "SELECT regionkey, COUNT(*) FROM remote.default.nation GROUP BY regionkey ORDER BY regionkey");
        MaterializedResult localGrouped = computeActual(
                "SELECT regionkey, COUNT(*) FROM tpch.tiny.nation GROUP BY regionkey ORDER BY regionkey");
        assertThat(remoteGrouped.getMaterializedRows()).isEqualTo(localGrouped.getMaterializedRows());

        // MIN/MAX
        MaterializedResult remoteMinMax = computeActual(
                "SELECT MIN(nationkey), MAX(nationkey) FROM remote.default.nation");
        MaterializedResult localMinMax = computeActual(
                "SELECT MIN(nationkey), MAX(nationkey) FROM tpch.tiny.nation");
        assertThat(remoteMinMax.getMaterializedRows()).isEqualTo(localMinMax.getMaterializedRows());

        // COUNT(DISTINCT)
        MaterializedResult remoteDistinct = computeActual(
                "SELECT COUNT(DISTINCT regionkey) FROM remote.default.nation");
        MaterializedResult localDistinct = computeActual(
                "SELECT COUNT(DISTINCT regionkey) FROM tpch.tiny.nation");
        assertThat(remoteDistinct.getOnlyValue()).isEqualTo(localDistinct.getOnlyValue());
    }

    @Test
    void testPredicatePushdown()
    {
        // Verify predicate filtering returns correct results matching local tpch
        MaterializedResult remote = computeActual(
                "SELECT name FROM remote.default.nation WHERE nationkey = 1");
        MaterializedResult local = computeActual(
                "SELECT name FROM tpch.tiny.nation WHERE nationkey = 1");
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());

        // Verify multiple predicates
        MaterializedResult remoteMulti = computeActual(
                "SELECT name FROM remote.default.nation WHERE regionkey = 1 AND nationkey > 5 ORDER BY name");
        MaterializedResult localMulti = computeActual(
                "SELECT name FROM tpch.tiny.nation WHERE regionkey = 1 AND nationkey > 5 ORDER BY name");
        assertThat(remoteMulti.getMaterializedRows()).isEqualTo(localMulti.getMaterializedRows());
    }

    @Test
    void testIntegrationJoinPushdown()
    {
        // Join between two tables on the same remote catalog — verify results match local
        MaterializedResult remote = computeActual(
                """
                SELECT n.name, r.name AS region_name
                FROM remote.default.nation n
                JOIN remote.default.region r ON n.regionkey = r.regionkey
                WHERE r.name = 'EUROPE'
                ORDER BY n.name
                """);
        MaterializedResult local = computeActual(
                """
                SELECT n.name, r.name AS region_name
                FROM tpch.tiny.nation n
                JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                WHERE r.name = 'EUROPE'
                ORDER BY n.name
                """);
        assertThat(remote.getMaterializedRows()).isEqualTo(local.getMaterializedRows());
    }

    @Test
    void testAutomaticJoinPushdownUsesRemoteStatistics()
    {
        String sql =
                """
                SELECT *
                FROM remote_tpch.tiny.nation n
                JOIN remote_tpch.tiny.region r ON n.regionkey = r.regionkey
                """;

        Session restrictive = Session.builder(getSession())
                .setCatalog("remote_tpch")
                .setSchema("tiny")
                .setCatalogSessionProperty("remote_tpch", "join_pushdown_strategy", "AUTOMATIC")
                .setCatalogSessionProperty("remote_tpch", "join_pushdown_automatic_max_join_to_tables_ratio", "0")
                .build();
        assertThat(query(restrictive, sql)).joinIsNotFullyPushedDown();

        Session permissive = Session.builder(restrictive)
                .setCatalogSessionProperty("remote_tpch", "join_pushdown_automatic_max_join_to_tables_ratio", "100")
                .build();
        assertThat(query(permissive, sql)).isFullyPushedDown();
    }

    @Test
    void testAutomaticJoinPushdownRequiresColumnStatistics()
    {
        MaterializedResult nationStats = computeActual("SHOW STATS FOR remote.default.nation");
        var tableStats = nationStats.getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst()
                .orElseThrow();
        assertThat(tableStats.getField(4)).isNotNull();

        var joinKeyStats = nationStats.getMaterializedRows().stream()
                .filter(row -> "regionkey".equals(row.getField(0)))
                .findFirst()
                .orElseThrow();
        assertThat(joinKeyStats.getField(2)).isNull();

        String sql =
                """
                SELECT *
                FROM remote.default.nation n
                JOIN remote.default.region r ON n.regionkey = r.regionkey
                """;

        Session eager = Session.builder(getSession())
                .setCatalog("remote")
                .setSchema("default")
                .setCatalogSessionProperty("remote", "join_pushdown_strategy", "EAGER")
                .build();
        assertThat(query(eager, sql)).isFullyPushedDown();

        Session automatic = Session.builder(eager)
                .setCatalogSessionProperty("remote", "join_pushdown_strategy", "AUTOMATIC")
                .setCatalogSessionProperty("remote", "join_pushdown_automatic_max_join_to_tables_ratio", "100")
                .build();
        assertThat(query(automatic, sql)).joinIsNotFullyPushedDown();
    }

    @Test
    void testJoinPushdownWithTransportColumn()
    {
        // Regression: the transport projection (CAST wrapping) used to be applied both when
        // synthesizing join sources and again at final scan, so a JSON-transport column
        // selected through a pushed-down join failed remotely (cast of already-cast varchar)
        Session eagerJoinPushdown = Session.builder(getSession())
                .setCatalogSessionProperty("remote", "join_pushdown_strategy", "EAGER")
                .build();
        String sql =
                """
                SELECT b.id, a.unsupported_col
                FROM remote.default.test_nested_unsupported_array a
                JOIN remote.default.test_nested_unsupported_map b ON a.id = b.id
                """;
        assertThat(query(eagerJoinPushdown, sql)).isFullyPushedDown();
        MaterializedResult result = computeActual(eagerJoinPushdown, sql);
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("id1");
        assertThat(result.getMaterializedRows().get(0).getField(1).toString()).contains("10:30:45.123+09:00");
    }

    @Test
    void testComplexFunctionDelegationKeepsLocaleSensitiveFilterLocal()
    {
        String sql =
                """
                SELECT regexp_extract(path, '/post/([0-9]+)', 1)
                FROM remote.default.test_delegation_log
                WHERE date_format(CAST(log_timestamp AS timestamp), '%Y-%m-%d') = '2024-01-15'
                    AND regexp_like(path, '^/post/')
                ORDER BY 1
                """;

        MaterializedResult result = computeActual(sql);
        assertThat(result.getOnlyColumnAsSet()).containsExactly("100", "200");

        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("ScanFilterProject");
        assertThat(explain).contains("filterPredicate = (date_format");
        assertThat(explain).contains("constraints=[ParameterizedExpression[expression=regexp_like");
    }

    @Test
    void testDateTruncProjectionRemoteDelegation()
    {
        String sql =
                """
                SELECT CAST(date_trunc('day', CAST(log_timestamp AS timestamp)) AS VARCHAR)
                FROM remote.default.test_delegation_log
                WHERE regexp_like(path, '^/post/')
                ORDER BY 1
                """;

        MaterializedResult result = computeActual(sql);
        assertThat(result.getMaterializedRows())
                .extracting(row -> row.getField(0))
                .containsExactly("2024-01-15 00:00:00.000", "2024-01-15 00:00:00.000");

        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("remote:Query[");
        assertThat(explain).contains("date_trunc");
        assertThat(explain).doesNotContain("ScanFilterProject");
    }

    @Test
    void testCharVarcharPredicateRemoteDelegation()
    {
        String sql =
                """
                SELECT id
                FROM remote.default.test_char_varchar_predicate
                WHERE v = c
                ORDER BY id
                """;

        MaterializedResult result = computeActual(sql);
        assertThat(result.getOnlyColumnAsSet()).containsExactly(1);

        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("constraints=[ParameterizedExpression[expression=(CAST(\"c\" AS varchar) = \"v\")");
        assertThat(explain).doesNotContain("ScanFilterProject");
    }

    @Test
    void testRemoteDelegationDisabled()
    {
        Session delegationOff = Session.builder(getSession())
                .setCatalogSessionProperty("remote", "remote_delegation_enabled", "false")
                .build();

        // Queries keep working through the baseline rewriter path, with
        // delegation-only expressions evaluated locally
        MaterializedResult result = computeActual(
                delegationOff,
                "SELECT regexp_extract(path, '/post/([0-9]+)', 1) FROM remote.default.test_delegation_log WHERE regexp_like(path, '^/post/') ORDER BY 1");
        assertThat(result.getOnlyColumnAsSet()).containsExactly("100", "200");
        String explain = computeActual(
                delegationOff,
                "EXPLAIN SELECT path FROM remote.default.test_delegation_log WHERE regexp_like(path, '^/post/')")
                .getOnlyValue().toString();
        assertThat(explain).contains("ScanFilter");
        assertThat(explain).contains("regexp_like");

        // Baseline pushdown stays available: tuple-domain predicates, the standard
        // comparison/arithmetic rewriter, and the aggregate rewriter
        assertThat(query(delegationOff, "SELECT name FROM remote.default.nation WHERE nationkey = 1"))
                .isFullyPushedDown();
        assertThat(query(delegationOff, "SELECT name FROM remote.default.nation WHERE nationkey + 1 = 2"))
                .isFullyPushedDown();
        assertThat(query(delegationOff, "SELECT count(*) FROM remote.default.nation"))
                .isFullyPushedDown();
        assertThat(query(delegationOff, "SELECT sum(x) FROM remote.default.test_decimal"))
                .isFullyPushedDown()
                .matches("VALUES CAST(1123.44 AS DECIMAL(38, 2))");
    }

    @Test
    void testLocaleSensitiveFormattingFallsBackLocally()
    {
        Session frenchSession = Session.builder(getSession())
                .setLocale(Locale.FRANCE)
                .build();
        String expected = computeActual(
                frenchSession,
                "SELECT format_datetime(TIMESTAMP '2024-01-15 10:30:45.123', 'MMMM')")
                .getOnlyValue()
                .toString();

        assertThat(computeActual(
                frenchSession,
                "SELECT format_datetime(CAST(log_timestamp AS timestamp), 'MMMM') FROM remote.default.test_delegation_log WHERE path = '/post/100'")
                .getOnlyValue())
                .isEqualTo(expected);
    }

    @Test
    void testSessionStartDependentCastFallsBackLocally()
    {
        String remoteTimeZone = computeActual(
                "SELECT * FROM TABLE(remote.system.query(query => 'SELECT current_timezone()'))")
                .getOnlyValue()
                .toString();
        String localTimeZone = remoteTimeZone.toUpperCase(Locale.ENGLISH).contains("UTC") || remoteTimeZone.toUpperCase(Locale.ENGLISH).contains("GMT")
                ? "Asia/Seoul"
                : "UTC";
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(localTimeZone))
                .build();

        String expected = computeActual(
                session,
                "SELECT CAST(CAST(TIMESTAMP '2024-01-15 10:30:45.123' AS TIME(3) WITH TIME ZONE) AS VARCHAR)")
                .getOnlyValue()
                .toString();
        assertThat(computeActual(
                session,
                "SELECT CAST(CAST(ts AS TIME(3) WITH TIME ZONE) AS VARCHAR) FROM remote.default.test_timestamp12_topn WHERE id = 'first'")
                .getOnlyValue())
                .isEqualTo(expected);
    }

    @Test
    void testTransportTopNRetainsLocalOrdering()
    {
        assertThat(query("SELECT id FROM remote.default.test_timestamp12_topn ORDER BY ts DESC LIMIT 2"))
                .ordered()
                .matches("VALUES 'third', 'second'")
                .isNotFullyPushedDown(TopNNode.class);

        String transportExplain = computeActual(
                "EXPLAIN SELECT id FROM remote.default.test_timestamp12_topn ORDER BY ts DESC LIMIT 2")
                .getOnlyValue()
                .toString();
        assertThat(transportExplain).contains("sortOrder=[");
        assertThat(transportExplain).contains("limit=2");

        String nativeExplain = computeActual(
                "EXPLAIN SELECT nationkey FROM remote.default.nation ORDER BY regionkey DESC LIMIT 2")
                .getOnlyValue()
                .toString();
        assertThat(nativeExplain).contains("sortOrder=[");
        assertThat(nativeExplain).contains("limit=2");
    }

    @Test
    void testComplexConstantPredicateFallsBackLocally()
    {
        // Regression: a complex-typed constant in a delegated predicate was emitted as
        // a QueryParameter with no bindable path, failing at scan time
        MaterializedResult result = computeActual(
                "SELECT path FROM remote.default.test_delegation_log WHERE contains(ARRAY[BIGINT '1', BIGINT '3'], regionkey) ORDER BY path");
        assertThat(result.getOnlyColumnAsSet()).containsExactly("/post/100", "/post/200");
    }

    @Test
    void testComplexConstantProjectionFallsBackLocally()
    {
        // Regression: same as above, through the projection path
        MaterializedResult result = computeActual(
                "SELECT concat(x, ARRAY[9, 9]) FROM remote.default.test_array_int");
        assertThat(result.getOnlyColumnAsSet()).containsExactlyInAnyOrder(
                List.of(1, 2, 3, 9, 9),
                List.of(4, 5, 9, 9));
    }

    @Test
    void testTimestamp12ConstantDelegationUsesTypedBinding()
    {
        assertThat(query(
                """
                SELECT date_diff(
                    'second',
                    TIMESTAMP '2024-01-15 10:30:45.123456789012',
                    CAST(log_timestamp AS TIMESTAMP(12)))
                FROM remote.default.test_delegation_log
                WHERE path = '/post/100'
                """))
                .isFullyPushedDown()
                .matches("VALUES BIGINT '0'");
    }

    @Test
    void testUnsupportedScalarConstantsFallBackLocally()
    {
        assertQuery(
                "SELECT contains(ARRAY[x], UUID '12345678-1234-1234-1234-123456789abc') FROM remote.default.test_uuid",
                "VALUES true");
        assertQuery(
                "SELECT contains(ARRAY[x], IPADDRESS '192.168.1.1') FROM remote.default.test_ipaddress",
                "VALUES true");
        assertQuery(
                "SELECT json_format(JSON '{\"key\":\"value\"}') FROM remote.default.test_json",
                "VALUES '{\"key\":\"value\"}'");
    }

    @Test
    void testFromIso8601TimestampFallsBackWhenRemoteTimeZoneDiffers()
    {
        String sql =
                """
                SELECT regexp_extract(path, '/post/([0-9]+)', 1)
                FROM remote.default.test_delegation_log
                WHERE date_trunc('day', CAST(log_timestamp AS timestamp)) = TIMESTAMP '2024-01-15 00:00:00'
                    AND regexp_like(path, '^/post/')
                    AND CAST(from_iso8601_timestamp(iso_timestamp) AT TIME ZONE 'Asia/Seoul' AS DATE) = DATE '2024-01-15'
                ORDER BY 1
                """;

        MaterializedResult result = computeActual(sql);
        assertThat(result.getOnlyColumnAsSet()).containsExactly("100", "200");

        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("ScanFilterProject");
        assertThat(explain).contains("from_iso8601_timestamp");
    }

    @Test
    void testRemoteSubtreeDelegatedForLocalJoin()
    {
        String sql =
                """
                SELECT r.name, pageviews.views
                FROM tpch.tiny.region r
                JOIN (
                    SELECT regionkey, count(*) AS views
                    FROM remote.default.test_delegation_log
                    WHERE regexp_like(path, '^/post/')
                    GROUP BY regionkey
                ) pageviews
                    ON pageviews.regionkey = r.regionkey
                """;

        MaterializedResult result = computeActual(sql);
        assertThat(result.getMaterializedRows()).hasSize(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo("AMERICA");
        assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo(2L);

        // The remote subtree (filter + aggregation) is delegated: the remote branch
        // collapses into a query relation scan and no local aggregation remains
        String explain = computeActual("EXPLAIN " + sql).getOnlyValue().toString();
        assertThat(explain).contains("remote:Query[");
        assertThat(explain).doesNotContain("Aggregate");
    }

    // =========================================================================
    // Read-only enforcement — verify INSERT/DELETE/UPDATE are blocked
    // =========================================================================

    @Test
    void testInsertBlocked()
    {
        assertAccessDenied("INSERT INTO remote.default.nation VALUES (99, 'TEST', 0, 'test')");
    }

    @Test
    void testCreateTableBlocked()
    {
        assertAccessDenied("CREATE TABLE remote.default.should_not_exist (id INTEGER)");
    }

    @Test
    void testDeleteBlocked()
    {
        assertAccessDenied("DELETE FROM remote.default.nation WHERE nationkey = 0");
    }

    @Test
    void testUpdateBlocked()
    {
        assertAccessDenied("UPDATE remote.default.nation SET name = 'X' WHERE nationkey = 0");
    }

    @Test
    void testDropTableBlocked()
    {
        assertAccessDenied("DROP TABLE remote.default.nation");
    }

    @Test
    void testCreateTableAsSelectBlocked()
    {
        assertAccessDenied("CREATE TABLE remote.default.should_not_exist_ctas AS SELECT 1 AS value");
    }

    @Test
    void testMergeBlocked()
    {
        assertAccessDenied(
                """
                MERGE INTO remote.default.nation target
                USING (VALUES (BIGINT '0', CAST('ALGERIA' AS VARCHAR), BIGINT '0', CAST('comment' AS VARCHAR))) source(nationkey, name, regionkey, comment)
                ON target.nationkey = source.nationkey
                WHEN MATCHED THEN UPDATE SET name = source.name
                """);
    }

    @Test
    void testTruncateBlocked()
    {
        assertAccessDenied("TRUNCATE TABLE remote.default.nation");
    }

    @Test
    void testAlterTableAddColumnBlocked()
    {
        assertAccessDenied("ALTER TABLE remote.default.nation ADD COLUMN should_not_exist BIGINT");
    }

    @Test
    void testCreateSchemaBlocked()
    {
        assertAccessDenied("CREATE SCHEMA remote.should_not_exist_schema");
    }

    @Test
    void testDropSchemaBlocked()
    {
        assertAccessDenied("DROP SCHEMA remote.empty_schema");
    }

    @Test
    void testCreateViewBlocked()
    {
        assertAccessDenied("CREATE VIEW remote.default.should_not_exist_view AS SELECT 1 AS value");
    }

    @Test
    void testCommentOnTableBlocked()
    {
        assertAccessDenied("COMMENT ON TABLE remote.default.nation IS 'comment'");
    }

    private void assertAccessDenied(String sql)
    {
        assertThatThrownBy(() -> computeActual(sql))
                .hasMessageContaining("Access Denied");
    }
}
