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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;

/**
 * Sets up two in-process Trino instances for testing the Trino connector:
 * <ul>
 *     <li><b>Remote</b>: Plain Trino with tpch + memory catalogs (acts as the remote data source)</li>
 *     <li><b>Local</b>: Trino with the Trino connector connecting to Remote</li>
 * </ul>
 */
public final class TrinoQueryRunner
{
    /**
     * Representative type coverage replacing the base testDataMappingSmokeTest,
     * which requires CREATE TABLE through the connector and is skipped for
     * read-only connectors. Fixtures are created remotely with a sample value,
     * a high value, and a NULL per type.
     */
    record DataMappingCase(String suffix, String type, String sampleLiteral, String highLiteral)
    {
        String columnName()
        {
            return "value_" + suffix;
        }

        String sampleValue()
        {
            return cast(sampleLiteral);
        }

        String highValue()
        {
            return cast(highLiteral);
        }

        String nullValue()
        {
            return cast("NULL");
        }

        private String cast(String literal)
        {
            return "CAST(" + literal + " AS " + type + ")";
        }
    }

    static final List<DataMappingCase> DATA_MAPPING_CASES = List.of(
            new DataMappingCase("boolean", "boolean", "false", "true"),
            new DataMappingCase("tinyint", "tinyint", "TINYINT '37'", "TINYINT '127'"),
            new DataMappingCase("smallint", "smallint", "SMALLINT '32123'", "SMALLINT '32767'"),
            new DataMappingCase("integer", "integer", "1274942432", "2147483647"),
            new DataMappingCase("bigint", "bigint", "BIGINT '312739231274942432'", "BIGINT '9223372036854775807'"),
            new DataMappingCase("real", "real", "REAL '567.123'", "REAL '999999.999'"),
            new DataMappingCase("double", "double", "DOUBLE '1234567890123.123'", "DOUBLE '9999999999999.999'"),
            new DataMappingCase("decimal_short", "decimal(3,1)", "12.3", "99.9"),
            new DataMappingCase("decimal_long", "decimal(30,5)", "DECIMAL '3141592653589793238462643.38327'", "DECIMAL '9999999999999999999999999.99999'"),
            new DataMappingCase("varchar", "varchar", "'test'", "'shall I compare thee to a summer''s day'"),
            new DataMappingCase("char", "char(3)", "'ab'", "'zzz'"),
            new DataMappingCase("varbinary", "varbinary", "X'12ab3f'", "X'ffffffffffffffffffff'"),
            new DataMappingCase("date", "date", "DATE '2020-02-12'", "DATE '9999-12-31'"),
            new DataMappingCase("time_3", "time(3)", "TIME '15:03:00.123'", "TIME '23:59:59.999'"),
            new DataMappingCase("time_6", "time(6)", "TIME '15:03:00.123456'", "TIME '23:59:59.999999'"),
            new DataMappingCase("time_12", "time(12)", "TIME '15:03:00.123456789012'", "TIME '23:59:59.999999999999'"),
            new DataMappingCase("timestamp_3", "timestamp(3)", "TIMESTAMP '2020-02-12 15:03:00.123'", "TIMESTAMP '2199-12-31 23:59:59.999'"),
            new DataMappingCase("timestamp_6", "timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00.123456'", "TIMESTAMP '2199-12-31 23:59:59.999999'"),
            new DataMappingCase("timestamp_9", "timestamp(9)", "TIMESTAMP '2020-02-12 15:03:00.123456789'", "TIMESTAMP '2199-12-31 23:59:59.999999999'"),
            new DataMappingCase("timestamp_12", "timestamp(12)", "TIMESTAMP '2020-02-12 15:03:00.123456789012'", "TIMESTAMP '2199-12-31 23:59:59.999999999999'"),
            new DataMappingCase("timestamptz_3", "timestamp(3) with time zone", "TIMESTAMP '2020-02-12 15:03:00.123 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999 +12:00'"),
            new DataMappingCase("timestamptz_6", "timestamp(6) with time zone", "TIMESTAMP '2020-02-12 15:03:00.123456 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999999 +12:00'"),
            new DataMappingCase("timestamptz_12", "timestamp(12) with time zone", "TIMESTAMP '2020-02-12 15:03:00.123456789012 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999999999999 +12:00'"),
            new DataMappingCase("uuid", "uuid", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", "UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff'"));

    private TrinoQueryRunner() {}

    public static Builder builder(DistributedQueryRunner remoteRunner)
    {
        return new Builder(remoteRunner);
    }

    /**
     * Creates the remote Trino instance with tpch and memory catalogs.
     */
    public static DistributedQueryRunner createRemoteQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog("tpch").setSchema("tiny").build())
                .setWorkerCount(0)
                // The full connector suite issues thousands of remote queries in parallel.
                // Keep completed queries available while JDBC fetches their result pages.
                .addExtraProperty("query.max-history", "1000")
                .build();
        try {
            remoteRunner.installPlugin(new TpchPlugin());
            remoteRunner.createCatalog("tpch", "tpch");
            remoteRunner.installPlugin(new MemoryPlugin());
            remoteRunner.createCatalog("memory", "memory");
            return remoteRunner;
        }
        catch (Exception | Error failure) {
            closeOnFailure(remoteRunner, failure);
            throw failure;
        }
    }

    /**
     * Populates the remote memory catalog with tpch tiny data so that
     * integration tests can access tpch tables through a single catalog connection.
     */
    static void populateTpchData(DistributedQueryRunner remoteRunner)
    {
        Session memorySession = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();
        for (String table : List.of("nation", "region", "orders", "customer", "lineitem", "part", "partsupp", "supplier")) {
            remoteRunner.execute(
                    memorySession,
                    "CREATE TABLE " + table + " AS SELECT * FROM tpch.tiny." + table);
        }
    }

    public static final class Builder
    {
        private final DistributedQueryRunner remoteRunner;
        private String remoteCatalog = "";
        private String defaultSchema = "tiny";
        private boolean withComplexTypeTestData;
        private boolean withRemoteTpchCatalog;
        private boolean withStatisticsDisabledCatalog;

        private Builder(DistributedQueryRunner remoteRunner)
        {
            this.remoteRunner = remoteRunner;
        }

        public Builder setRemoteCatalog(String remoteCatalog)
        {
            this.remoteCatalog = remoteCatalog;
            return this;
        }

        public Builder setDefaultSchema(String defaultSchema)
        {
            this.defaultSchema = defaultSchema;
            return this;
        }

        public Builder withComplexTypeTestData()
        {
            this.withComplexTypeTestData = true;
            return this;
        }

        /**
         * Also creates a "remote_tpch" catalog pointing at the remote TPCH catalog.
         * TPCH exposes complete table and column statistics, unlike Memory, so it is
         * used to exercise cost-based automatic join pushdown.
         */
        public Builder withRemoteTpchCatalog()
        {
            this.withRemoteTpchCatalog = true;
            return this;
        }

        /**
         * Also creates a "remote_stats_disabled" catalog pointing at the same remote
         * with {@code statistics.enabled=false}, without booting another cluster.
         */
        public Builder withStatisticsDisabledCatalog()
        {
            this.withStatisticsDisabledCatalog = true;
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            int remotePort = remoteRunner.getCoordinator().getBaseUrl().getPort();

            Session defaultSession = testSessionBuilder()
                    .setCatalog("remote")
                    .setSchema(defaultSchema)
                    .build();

            // The local runner keeps the default worker count so inherited framework
            // tests exercise a genuinely distributed plan; the remote runner stays at
            // zero workers to bound test cost.
            DistributedQueryRunner localRunner = DistributedQueryRunner.builder(defaultSession)
                    .addExtraProperty("retry-policy", "NONE")
                    .build();
            try {
                localRunner.installPlugin(new TpchPlugin());
                localRunner.createCatalog("tpch", "tpch");

                String connectionUrl = "jdbc:trino://localhost:" + remotePort;
                if (!remoteCatalog.isEmpty()) {
                    connectionUrl += "/" + remoteCatalog;
                }

                Map<String, String> properties = createConnectorProperties(connectionUrl);

                localRunner.installPlugin(new TrinoPlugin());
                localRunner.createCatalog("remote", "trino", properties);

                if (withRemoteTpchCatalog) {
                    localRunner.createCatalog(
                            "remote_tpch",
                            "trino",
                            createConnectorProperties("jdbc:trino://localhost:" + remotePort + "/tpch"));
                }

                if (withStatisticsDisabledCatalog) {
                    Map<String, String> statsDisabledProperties = ImmutableMap.<String, String>builder()
                            .putAll(properties)
                            .put("statistics.enabled", "false")
                            .buildOrThrow();
                    localRunner.createCatalog("remote_stats_disabled", "trino", statsDisabledProperties);
                }

                if (withComplexTypeTestData) {
                    createTestData(remoteRunner);
                }

                // Successful construction transfers ownership of the remote runner.
                localRunner.registerResource(remoteRunner);
                return localRunner;
            }
            catch (Exception | Error failure) {
                closeOnFailure(localRunner, failure);
                throw failure;
            }
        }

        private Map<String, String> createConnectorProperties(String connectionUrl)
        {
            return ImmutableMap.<String, String>builder()
                    .put("connection-url", connectionUrl)
                    .put("connection-user", "test")
                    .buildOrThrow();
        }
    }

    static void closeOnFailure(AutoCloseable closeable, Throwable failure)
    {
        try {
            closeable.close();
        }
        catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    /**
     * Creates test tables with various types on the remote Trino's memory catalog.
     */
    static void createTestData(DistributedQueryRunner remoteRunner)
    {
        Session memorySession = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();

        remoteRunner.execute(memorySession, "CREATE SCHEMA alt");
        remoteRunner.execute(memorySession, "CREATE SCHEMA empty_schema");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE alt.access_log AS SELECT * FROM (VALUES BIGINT '1', BIGINT '2') AS t(id)");

        // --- ARRAY types ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_varchar AS SELECT x FROM (VALUES ARRAY['a','b','c'], ARRAY['d'], ARRAY[]) AS t(x)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_int AS SELECT x FROM (VALUES ARRAY[1,2,3], ARRAY[4,5]) AS t(x)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_nulls AS SELECT x FROM (VALUES ARRAY['a', NULL, 'c'], CAST(NULL AS ARRAY(VARCHAR))) AS t(x)");

        // --- MAP types ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_map_vv AS SELECT x FROM (VALUES MAP(ARRAY['k1'], ARRAY['v1']), MAP(ARRAY['a','b'], ARRAY['1','2'])) AS t(x)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_map_nulls AS SELECT x FROM (VALUES MAP(ARRAY['a','b'], ARRAY['1', NULL])) AS t(x)");

        // --- ROW types ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_row AS SELECT CAST(ROW('Alice', 30) AS ROW(name VARCHAR, age INTEGER)) AS x UNION ALL SELECT CAST(ROW('Bob', 25) AS ROW(name VARCHAR, age INTEGER))");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_row_null AS SELECT CAST(ROW('Alice', NULL) AS ROW(name VARCHAR, age INTEGER)) AS x UNION ALL SELECT CAST(NULL AS ROW(name VARCHAR, age INTEGER))");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_row_temporal_null AS
                SELECT * FROM (
                    VALUES
                        (1, CAST(NULL AS ROW(ts TIMESTAMP(3), name VARCHAR))),
                        (2, CAST(ROW(CAST(NULL AS TIMESTAMP(3)), CAST(NULL AS VARCHAR)) AS ROW(ts TIMESTAMP(3), name VARCHAR))),
                        (3, CAST(ROW(TIMESTAMP '2024-01-15 10:30:45.123', 'ok') AS ROW(ts TIMESTAMP(3), name VARCHAR)))
                ) AS t(id, x)
                """);
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_array_row_temporal_null AS
                SELECT ARRAY[
                    CAST(NULL AS ROW(ts TIMESTAMP(3), name VARCHAR)),
                    CAST(ROW(CAST(NULL AS TIMESTAMP(3)), CAST(NULL AS VARCHAR)) AS ROW(ts TIMESTAMP(3), name VARCHAR))
                ] AS x
                """);

        // --- Nested: array(map(varchar, varchar)) ---
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_array_of_map AS
                SELECT ARRAY[
                    MAP(ARRAY['type','post_id','nation_key'], ARRAY['pageview','p1','2']),
                    MAP(ARRAY['type','post_id','nation_key'], ARRAY['click','p2','3'])
                ] AS evts
                """);

        // --- Deeply nested: row(svc varchar, evts array(map(varchar, varchar))) ---
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_nested_row AS
                SELECT CAST(ROW('article', ARRAY[
                    MAP(ARRAY['type'], ARRAY['pageview']),
                    MAP(ARRAY['type'], ARRAY['click'])
                ]) AS ROW(svc VARCHAR, evts ARRAY(MAP(VARCHAR, VARCHAR)))) AS d
                """);

        // --- Boolean ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_boolean AS SELECT x FROM (VALUES true, false) AS t(x)");

        // --- Decimal ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_decimal AS SELECT x FROM (VALUES DECIMAL '123.45', DECIMAL '999.99') AS t(x)");

        // --- NUMBER ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_number AS SELECT x FROM (VALUES NUMBER '0.1', NUMBER '3.1415', NUMBER '20050910133100123') AS t(x)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_number_special AS SELECT x FROM (VALUES NUMBER 'NaN', NUMBER '+Infinity', NUMBER '-Infinity') AS t(x)");

        // --- NULL varchar ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_null_varchar AS SELECT x FROM (VALUES 'hello', CAST(NULL AS VARCHAR)) AS t(x)");

        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_delegation_log AS
                SELECT * FROM (
                    VALUES
                        ('2024-01-15 10:30:45.123', '2024-01-15T01:30:45Z', '/post/100', BIGINT '1'),
                        ('2024-01-15 12:00:00.000', '2024-01-15T03:00:00Z', '/post/200', BIGINT '1'),
                        ('2024-01-16 11:00:00.000', '2024-01-16T02:00:00Z', '/search', BIGINT '2')
                ) AS t(log_timestamp, iso_timestamp, path, regionkey)
                """);

        // --- Date ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_date AS SELECT x FROM (VALUES DATE '2024-01-15', DATE '1999-12-31') AS t(x)");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_historical_date AS
                SELECT * FROM (
                    VALUES
                        (1, DATE '-0001-01-01'),
                        (2, DATE '0000-01-01'),
                        (3, DATE '1582-10-10'),
                        (4, DATE '+10000-01-01')
                ) AS t(id, x)
                """);

        // --- Timestamp with precision ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_timestamp6 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456' AS TIMESTAMP(6)) AS x");

        // --- UUID ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_uuid AS SELECT UUID '12345678-1234-1234-1234-123456789abc' AS x");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_uuid_null AS
                SELECT * FROM (
                    VALUES
                        (1, UUID '12345678-1234-1234-1234-123456789abc'),
                        (2, CAST(NULL AS UUID))
                ) AS t(id, x)
                """);

        // --- Array of dates (complex type with temporal inner type) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_date AS SELECT ARRAY[DATE '2024-01-01', DATE '2024-06-15'] AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_historical_date AS SELECT ARRAY[DATE '-0001-01-01', DATE '0000-01-01', DATE '1582-10-10', DATE '+10000-01-01'] AS x");

        // --- Map with decimal values ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_map_decimal AS SELECT MAP(ARRAY['price','tax'], ARRAY[DECIMAL '99.99', DECIMAL '7.50']) AS x");

        // --- Array of timestamps (temporal inside complex) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_timestamp AS SELECT ARRAY[TIMESTAMP '2024-01-15 10:30:45.123456', TIMESTAMP '2024-06-15 23:59:59.000000'] AS x");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_timestamp12_topn AS
                SELECT * FROM (
                    VALUES
                        ('first', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789010' AS TIMESTAMP(12))),
                        ('second', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789011' AS TIMESTAMP(12))),
                        ('third', CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)))
                ) AS t(id, ts)
                """);

        // --- Array of timestamp with time zone ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_tstz AS SELECT ARRAY[TIMESTAMP '2024-01-15 10:30:45.123 UTC', TIMESTAMP '2024-06-15 12:00:00.000 +09:00'] AS x");

        // --- Map with UUID values ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_map_uuid AS SELECT MAP(ARRAY['id1','id2'], ARRAY[UUID '12345678-1234-1234-1234-123456789abc', UUID 'abcdefab-cdef-abcd-efab-cdefabcdefab']) AS x");

        // --- Nested row inside row ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_row_in_row AS SELECT CAST(ROW(ROW(1, 'hello'), 'outer') AS ROW(inner_val ROW(x INTEGER, y VARCHAR), label VARCHAR)) AS d");

        // --- Quoted row field names ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_quoted_row_field AS SELECT CAST(ROW('value', 42) AS ROW(\"my field\" VARCHAR, \"my count\" INTEGER)) AS d");

        // --- TIME with various precisions ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_time AS SELECT x FROM (VALUES TIME '10:30:45', TIME '23:59:59') AS t(x)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_time6 AS SELECT CAST(TIME '10:30:45.123456' AS TIME(6)) AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_time12 AS SELECT CAST(TIME '10:30:45.123456789012' AS TIME(12)) AS x");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_array_time_precision AS
                SELECT
                    ARRAY[CAST(TIME '10:30:45' AS TIME(0))] AS p0,
                    ARRAY[CAST(TIME '10:30:45.123' AS TIME(3))] AS p3,
                    ARRAY[CAST(TIME '10:30:45.123456' AS TIME(6))] AS p6,
                    ARRAY[CAST(TIME '10:30:45.123456789' AS TIME(9))] AS p9
                """);
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_timetz3 AS SELECT CAST(TIME '10:30:45.123 +09:00' AS TIME(3) WITH TIME ZONE) AS x");

        // --- CHAR type ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_char AS SELECT CAST('abc' AS CHAR(10)) AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_char AS SELECT ARRAY[CAST('abc' AS CHAR(10))] AS x");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_char_varchar_predicate AS
                SELECT * FROM (
                    VALUES
                        (1, CAST('a' AS CHAR(3)), VARCHAR 'a'),
                        (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '),
                        (3, CAST('b' AS CHAR(3)), VARCHAR 'b  ')
                ) AS t(id, c, v)
                """);

        // --- VARBINARY type ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_varbinary AS SELECT x FROM (VALUES X'48656C6C6F', X'') AS t(x)");

        // --- TIMESTAMP WITH TIME ZONE (top-level, not nested) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tstz AS SELECT TIMESTAMP '2024-01-15 10:30:45.123 UTC' AS x");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_tstz_dst_fold AS
                SELECT * FROM (
                    VALUES
                        (
                            1,
                            CAST(at_timezone(TIMESTAMP '2022-10-30 00:30:00.123 UTC', 'Europe/Warsaw') AS TIMESTAMP(3) WITH TIME ZONE),
                            CAST(at_timezone(TIMESTAMP '2022-10-30 00:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE)
                        ),
                        (
                            2,
                            CAST(at_timezone(TIMESTAMP '2022-10-30 01:30:00.123 UTC', 'Europe/Warsaw') AS TIMESTAMP(3) WITH TIME ZONE),
                            CAST(at_timezone(TIMESTAMP '2022-10-30 01:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE)
                        )
                ) AS t(id, p3, p12)
                """);
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_nested_tstz_dst_fold AS
                WITH fold AS (
                    SELECT
                        CAST(at_timezone(TIMESTAMP '2022-10-30 00:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE) AS earlier,
                        CAST(at_timezone(TIMESTAMP '2022-10-30 01:30:00.123456789012 UTC', 'Europe/Warsaw') AS TIMESTAMP(12) WITH TIME ZONE) AS later
                )
                SELECT
                    ARRAY[earlier, later] AS array_value,
                    MAP(ARRAY['earlier', 'later'], ARRAY[earlier, later]) AS map_value,
                    CAST(ROW(earlier, later) AS ROW(earlier TIMESTAMP(12) WITH TIME ZONE, later TIMESTAMP(12) WITH TIME ZONE)) AS row_value
                FROM fold
                """);
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_tstz_historical_zone AS
                SELECT CAST(
                    at_timezone(TIMESTAMP '1890-01-01 00:00:00.000 UTC', 'Europe/Paris')
                    AS TIMESTAMP(3) WITH TIME ZONE) AS value
                """);

        // --- Long DECIMAL (precision > 18, uses Decimals.encodeScaledValue path) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_long_decimal AS SELECT CAST(12345678901234567890.12345 AS DECIMAL(38, 5)) AS x");

        // --- Long TIMESTAMP (precision > 6, uses LongTimestamp path) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_timestamp9 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456789' AS TIMESTAMP(9)) AS x");

        // --- Long TIMESTAMP WITH TIME ZONE (precision > 3) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tstz6 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456 UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS x");

        // --- JSON type (native type mapping) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_json AS SELECT JSON '{\"key\": \"value\", \"num\": 42}' AS x");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_json_null AS
                SELECT * FROM (
                    VALUES
                        (1, JSON '{\"key\": \"value\"}'),
                        (2, CAST(NULL AS JSON))
                ) AS t(id, x)
                """);

        // --- IPADDRESS type (native type mapping) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_ipaddress AS SELECT IPADDRESS '192.168.1.1' AS x");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_ipaddress_null AS
                SELECT * FROM (
                    VALUES
                        (1, IPADDRESS '192.168.1.1'),
                        (2, CAST(NULL AS IPADDRESS))
                ) AS t(id, x)
                """);

        // --- TSTZ with various precisions (regression: ensure all 0-9 work) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tstz0 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45 UTC' AS TIMESTAMP(0) WITH TIME ZONE) AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tstz1 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.1 UTC' AS TIMESTAMP(1) WITH TIME ZONE) AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tstz2 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.12 UTC' AS TIMESTAMP(2) WITH TIME ZONE) AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tstz9 AS SELECT CAST(TIMESTAMP '2024-01-15 10:30:45.123456789 UTC' AS TIMESTAMP(9) WITH TIME ZONE) AS x");

        // --- Nested JSON and IPADDRESS inside complex types ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_array_json AS SELECT ARRAY[JSON '{\"a\":1}', JSON '{\"b\":2}'] AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_map_ipaddress AS SELECT MAP(ARRAY['server1','server2'], ARRAY[IPADDRESS '192.168.1.1', IPADDRESS '10.0.0.1']) AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_row_json AS SELECT CAST(ROW('test', JSON '{\"key\":\"val\"}') AS ROW(label VARCHAR, data JSON)) AS x");

        // --- TSTZ precision > 9: unsupported (Java ZonedDateTime caps at nanosecond) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tstz12 AS SELECT CAST('id1' AS VARCHAR) AS id, CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012 UTC' AS TIMESTAMP(12) WITH TIME ZONE) AS unsupported_col");

        // --- Plain TIMESTAMP precision > 9: unsupported (JDBC caps at nanosecond) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_ts12 AS SELECT CAST('id1' AS VARCHAR) AS id, CAST(TIMESTAMP '2024-01-15 10:30:45.123456789012' AS TIMESTAMP(12)) AS unsupported_col");

        // --- Top-level interval transport fallback ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_interval_day_to_second AS SELECT INTERVAL '2 03:04:05.678' DAY TO SECOND AS x");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_interval_year_to_month AS SELECT INTERVAL '1-6' YEAR TO MONTH AS x");

        // --- Top-level sketch transport fallback via VARBINARY ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_hyperloglog AS SELECT approx_set(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_qdigest AS SELECT qdigest_agg(v) AS x FROM (VALUES BIGINT '1', BIGINT '2', BIGINT '3', BIGINT '4', BIGINT '5') t(v)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_tdigest AS SELECT tdigest_agg(v) AS x FROM (VALUES DOUBLE '1.0', DOUBLE '2.0', DOUBLE '3.0', DOUBLE '4.0', DOUBLE '5.0') t(v)");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_setdigest AS SELECT make_set_digest(v) AS x FROM (VALUES 1, 2, 3, 4, 5) t(v)");

        // --- Nested unsupported complex types ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_nested_unsupported_array AS SELECT CAST('id1' AS VARCHAR) AS id, ARRAY[CAST(TIME '10:30:45.123 +09:00' AS TIME(3) WITH TIME ZONE)] AS unsupported_col");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_nested_unsupported_map AS SELECT CAST('id1' AS VARCHAR) AS id, MAP(ARRAY['duration'], ARRAY[INTERVAL '1' DAY]) AS unsupported_col");
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_nested_unsupported_map_int_key AS SELECT CAST('id1' AS VARCHAR) AS id, MAP(ARRAY[1, 2], ARRAY[INTERVAL '1' DAY, INTERVAL '2' DAY]) AS unsupported_col");
        remoteRunner.execute(memorySession,
                """
                CREATE TABLE test_nested_unsupported_map_row_key AS
                SELECT
                    CAST('id1' AS VARCHAR) AS id,
                    MAP(
                        ARRAY[
                            CAST(ROW(1) AS ROW(x INTEGER)),
                            CAST(ROW(2) AS ROW(x INTEGER))
                        ],
                        ARRAY[INTERVAL '1' DAY, INTERVAL '2' DAY]
                    ) AS unsupported_col
                """);
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_nested_unsupported_row AS SELECT CAST('id1' AS VARCHAR) AS id, CAST(ROW(INTERVAL '1' DAY) AS ROW(x INTERVAL DAY TO SECOND)) AS unsupported_col");

        // --- Opaque unsupported type without a transport rule (for unsupported-type-handling fallback) ---
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE test_unsupported_color AS SELECT rgb(255, 0, 0) AS x");

        // --- Representative data-mapping fixture (sample/high/null per type) ---
        String columnNames = DATA_MAPPING_CASES.stream()
                .map(DataMappingCase::columnName)
                .collect(joining(", "));
        String sampleValues = DATA_MAPPING_CASES.stream()
                .map(DataMappingCase::sampleValue)
                .collect(joining(", "));
        String highValues = DATA_MAPPING_CASES.stream()
                .map(DataMappingCase::highValue)
                .collect(joining(", "));
        String nullValues = DATA_MAPPING_CASES.stream()
                .map(DataMappingCase::nullValue)
                .collect(joining(", "));
        remoteRunner.execute(
                memorySession,
                "CREATE TABLE data_mapping AS SELECT * FROM (VALUES " +
                        "(1, " + sampleValues + "), " +
                        "(2, " + highValues + "), " +
                        "(3, " + nullValues + ")) " +
                        "AS t(id, " + columnNames + ")");
    }
}
