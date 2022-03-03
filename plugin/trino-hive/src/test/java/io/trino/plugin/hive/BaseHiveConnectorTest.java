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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryInfo;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.planprinter.IoPlanPrinter.ColumnConstraint;
import io.trino.sql.planner.planprinter.IoPlanPrinter.EstimatedStatsAndCost;
import io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedDomain;
import io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedMarker;
import io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedRange;
import io.trino.sql.planner.planprinter.IoPlanPrinter.IoPlan;
import io.trino.sql.planner.planprinter.IoPlanPrinter.IoPlan.TableColumnInfo;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.type.TypeDeserializer;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.COLOCATED_JOIN;
import static io.trino.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static io.trino.SystemSessionProperties.DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.GROUPED_EXECUTION;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.USE_TABLE_SCAN_NODE_PARTITIONING;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.HiveQueryRunner.createBucketedSession;
import static io.trino.plugin.hive.HiveTableProperties.AUTO_PURGE;
import static io.trino.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.util.HiveUtil.columnExtraInfo;
import static io.trino.spi.security.Identity.ofUser;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedMarker.Bound.ABOVE;
import static io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedMarker.Bound.EXACTLY;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.sql.tree.ExplainType.Type.DISTRIBUTED;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertFile;

public abstract class BaseHiveConnectorTest
        extends BaseConnectorTest
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    private final String catalog;
    private final Session bucketedSession;

    protected BaseHiveConnectorTest()
    {
        this.catalog = HIVE_CATALOG;
        this.bucketedSession = createBucketedSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin"))));
    }

    protected static QueryRunner createHiveQueryRunner(Map<String, String> extraProperties, Map<String, String> exchangeManagerProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .setExtraProperties(extraProperties)
                .setHiveProperties(ImmutableMap.of(
                        "hive.allow-register-partition-procedure", "true",
                        // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                        "hive.writer-sort-buffer-size", "1MB",
                        // Make weighted split scheduling more conservative to avoid OOMs in test
                        "hive.minimum-assigned-split-weight", "0.5"))
                .addExtraProperty("legacy.allow-set-view-authorization", "true")
                .setExchangeManagerProperties(exchangeManagerProperties)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();

        // extra catalog with NANOSECOND timestamp precision
        queryRunner.createCatalog(
                "hive_timestamp_nanos",
                "hive",
                ImmutableMap.of("hive.timestamp-precision", "NANOSECONDS"));
        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_DELETE:
                return true;

            case SUPPORTS_UPDATE:
                return true;

            case SUPPORTS_MULTI_STATEMENT_WRITES:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessage("Hive update is only supported for ACID transactional tables");
    }

    @Override
    public void testUpdateRowConcurrently()
            throws Exception
    {
        // TODO (https://github.com/trinodb/trino/issues/10518) test this with a TestHiveConnectorTest version that creates ACID tables by default, or in some other way
        assertThatThrownBy(super::testUpdateRowConcurrently)
                .hasMessage("Unexpected concurrent update failure")
                .getCause()
                .hasMessage("Hive update is only supported for ACID transactional tables");
    }

    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    public void testDeleteWithVarcharPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Test(dataProvider = "queryPartitionFilterRequiredSchemasDataProvider")
    public void testRequiredPartitionFilter(String queryPartitionFilterRequiredSchemas)
    {
        Session session = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .setCatalogSessionProperty("hive", "query_partition_filter_required", "true")
                .setCatalogSessionProperty("hive", "query_partition_filter_required_schemas", queryPartitionFilterRequiredSchemas)
                .build();

        assertUpdate(session, "CREATE TABLE test_required_partition_filter(id integer, a varchar, b varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO test_required_partition_filter(id, a, ds) VALUES (1, 'a', '1')", 1);
        String filterRequiredMessage = "Filter required on tpch\\.test_required_partition_filter for at least one partition column: ds";

        // no partition filter
        assertQueryFails(session, "SELECT id FROM test_required_partition_filter WHERE a = '1'", filterRequiredMessage);
        assertQueryFails(session, "EXPLAIN SELECT id FROM test_required_partition_filter WHERE a = '1'", filterRequiredMessage);
        assertQueryFails(session, "EXPLAIN ANALYZE SELECT id FROM test_required_partition_filter WHERE a = '1'", filterRequiredMessage);

        // partition filter that gets removed by planner
        assertQueryFails(session, "SELECT id FROM test_required_partition_filter WHERE ds IS NOT NULL OR true", filterRequiredMessage);

        // equality partition filter
        assertQuery(session, "SELECT id FROM test_required_partition_filter WHERE ds = '1'", "SELECT 1");
        computeActual(session, "EXPLAIN SELECT id FROM test_required_partition_filter WHERE ds = '1'");

        // IS NOT NULL partition filter
        assertQuery(session, "SELECT id FROM test_required_partition_filter WHERE ds IS NOT NULL", "SELECT 1");

        // predicate involving a CAST (likely unwrapped)
        assertQuery(session, "SELECT id FROM test_required_partition_filter WHERE CAST(ds AS integer) = 1", "SELECT 1");

        // partition predicate in outer query only
        assertQuery(session, "SELECT id FROM (SELECT * FROM test_required_partition_filter WHERE CAST(id AS smallint) = 1) WHERE CAST(ds AS integer) = 1", "select 1");
        computeActual(session, "EXPLAIN SELECT id FROM (SELECT * FROM test_required_partition_filter WHERE CAST(id AS smallint) = 1) WHERE CAST(ds AS integer) = 1");

        // ANALYZE
        assertQueryFails(session, "ANALYZE test_required_partition_filter", filterRequiredMessage);
        assertQueryFails(session, "EXPLAIN ANALYZE test_required_partition_filter", filterRequiredMessage);

        assertUpdate(session, "ANALYZE test_required_partition_filter WITH (partitions=ARRAY[ARRAY['1']])", 1);
        computeActual(session, "EXPLAIN ANALYZE test_required_partition_filter WITH (partitions=ARRAY[ARRAY['1']])");

        assertUpdate(session, "DROP TABLE test_required_partition_filter");
    }

    @Test(dataProvider = "queryPartitionFilterRequiredSchemasDataProvider")
    public void testRequiredPartitionFilterInferred(String queryPartitionFilterRequiredSchemas)
    {
        Session session = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .setCatalogSessionProperty("hive", "query_partition_filter_required", "true")
                .setCatalogSessionProperty("hive", "query_partition_filter_required_schemas", queryPartitionFilterRequiredSchemas)
                .build();

        assertUpdate(session, "CREATE TABLE test_partition_filter_inferred_left(id integer, a varchar, b varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");
        assertUpdate(session, "CREATE TABLE test_partition_filter_inferred_right(id integer, a varchar, b varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");

        assertUpdate(session, "INSERT INTO test_partition_filter_inferred_left(id, a, ds) VALUES (1, 'a', '1')", 1);
        assertUpdate(session, "INSERT INTO test_partition_filter_inferred_right(id, a, ds) VALUES (1, 'a', '1')", 1);

        // Join on partition column allowing filter inference for the other table
        assertQuery(
                session,
                "SELECT l.id, r.id FROM test_partition_filter_inferred_left l JOIN test_partition_filter_inferred_right r ON l.ds = r.ds WHERE l.ds = '1'",
                "SELECT 1, 1");

        // Join on non-partition column
        assertQueryFails(
                session,
                "SELECT l.ds, r.ds FROM test_partition_filter_inferred_left l JOIN test_partition_filter_inferred_right r ON l.id = r.id WHERE l.ds = '1'",
                "Filter required on tpch\\.test_partition_filter_inferred_right for at least one partition column: ds");

        assertUpdate(session, "DROP TABLE test_partition_filter_inferred_left");
        assertUpdate(session, "DROP TABLE test_partition_filter_inferred_right");
    }

    @DataProvider
    public Object[][] queryPartitionFilterRequiredSchemasDataProvider()
    {
        return new Object[][] {
                {"[]"},
                {"[\"tpch\"]"}
        };
    }

    @Test
    public void testRequiredPartitionFilterAppliedOnDifferentSchema()
    {
        String schemaName = "schema_" + randomTableSuffix();
        Session session = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .setCatalogSessionProperty("hive", "query_partition_filter_required", "true")
                .setCatalogSessionProperty("hive", "query_partition_filter_required_schemas", format("[\"%s\"]", schemaName))
                .build();

        getQueryRunner().execute("CREATE SCHEMA " + schemaName);

        try (TestTable table = new TestTable(
                new TrinoSqlExecutor(getQueryRunner(), session),
                "test_required_partition_filter_",
                "(id integer, a varchar, b varchar) WITH (partitioned_by = ARRAY['b'])",
                ImmutableList.of("1, '1', 'b'"))) {
            // no partition filter
            assertQuery(session, format("SELECT id FROM %s WHERE a = '1'", table.getName()), "SELECT 1");
            computeActual(session, format("EXPLAIN SELECT id FROM %s WHERE a = '1'", table.getName()));
            computeActual(session, format("EXPLAIN ANALYZE SELECT id FROM %s WHERE a = '1'", table.getName()));

            // partition filter that gets removed by planner
            assertQuery(session, format("SELECT id FROM %s WHERE b IS NOT NULL OR true", table.getName()), "SELECT 1");

            // Join on non-partition column
            assertUpdate(session, format("CREATE TABLE %s.%s_right (id integer, a varchar, b varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])", schemaName, table.getName()));

            assertUpdate(session, format("INSERT INTO %s.%s_right (id, a, ds) VALUES (1, 'a', '1')", schemaName, table.getName()), 1);

            assertQueryFails(
                    session,
                    format("SELECT count(*) FROM %2$s l JOIN %s.%2$s_right r ON l.id = r.id WHERE r.a = 'a'", schemaName, table.getName()),
                    format("Filter required on %s\\.%s_right for at least one partition column: ds", schemaName, table.getName()));

            assertQuery(session, format("SELECT count(*) FROM %2$s l JOIN %s.%2$s_right r ON l.id = r.id WHERE r.ds = '1'", schemaName, table.getName()), "SELECT 1");

            assertUpdate(session, format("DROP TABLE %s.%s_right", schemaName, table.getName()));
        }
        getQueryRunner().execute("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testInvalidValueForQueryPartitionFilterRequiredSchemas()
    {
        assertQueryFails(
                "SET SESSION hive.query_partition_filter_required_schemas = ARRAY['tpch', null]",
                "line 1:1: Invalid null or empty value in query_partition_filter_required_schemas property");

        assertQueryFails(
                "SET SESSION hive.query_partition_filter_required_schemas = ARRAY['tpch', '']",
                "line 1:1: Invalid null or empty value in query_partition_filter_required_schemas property");
    }

    @Test
    public void testNaNPartition()
    {
        // Only NaN partition
        assertUpdate("DROP TABLE IF EXISTS test_nan_partition");
        assertUpdate("CREATE TABLE test_nan_partition(a varchar, d double) WITH (partitioned_by = ARRAY['d'])");
        assertUpdate("INSERT INTO test_nan_partition VALUES ('b', nan())", 1);

        assertQuery(
                "SELECT a, d, regexp_replace(\"$path\", '.*(/[^/]*/[^/]*/)[^/]*', '...$1...') FROM test_nan_partition",
                "VALUES ('b', SQRT(-1), '.../test_nan_partition/d=NaN/...')"); // SQRT(-1) is H2's recommended way to obtain NaN
        assertQueryReturnsEmptyResult("SELECT a FROM test_nan_partition JOIN (VALUES 33e0) u(x) ON d = x");
        assertQueryReturnsEmptyResult("SELECT a FROM test_nan_partition JOIN (VALUES 33e0) u(x) ON d = x OR rand() = 42");
        assertQueryReturnsEmptyResult("SELECT * FROM test_nan_partition t1 JOIN test_nan_partition t2 ON t1.d = t2.d");
        assertQuery(
                "SHOW STATS FOR test_nan_partition",
                "VALUES " +
                        "('a', 1, 1, 0, null, null, null), " +
                        "('d', null, 1, 0, null, null, null), " +
                        "(null, null, null, null, 1, null, null)");

        assertUpdate("DROP TABLE IF EXISTS test_nan_partition");

        // NaN partition and other partitions
        assertUpdate("CREATE TABLE test_nan_partition(a varchar, d double) WITH (partitioned_by = ARRAY['d'])");
        assertUpdate("INSERT INTO test_nan_partition VALUES ('a', 42e0), ('b', nan())", 2);

        assertQuery(
                "SELECT a, d, regexp_replace(\"$path\", '.*(/[^/]*/[^/]*/)[^/]*', '...$1...') FROM test_nan_partition",
                "VALUES " +
                        "  ('a', 42, '.../test_nan_partition/d=42.0/...'), " +
                        "  ('b', SQRT(-1), '.../test_nan_partition/d=NaN/...')"); // SQRT(-1) is H2's recommended way to obtain NaN
        assertQueryReturnsEmptyResult("SELECT a FROM test_nan_partition JOIN (VALUES 33e0) u(x) ON d = x");
        assertQueryReturnsEmptyResult("SELECT a FROM test_nan_partition JOIN (VALUES 33e0) u(x) ON d = x OR rand() = 42");
        assertQuery("SELECT * FROM test_nan_partition t1 JOIN test_nan_partition t2 ON t1.d = t2.d", "VALUES ('a', 42, 'a', 42)");
        assertQuery(
                "SHOW STATS FOR test_nan_partition",
                "VALUES " +
                        "('a', 2, 1, 0, null, null, null), " +
                        "('d', null, 2, 0, null, null, null), " +
                        "(null, null, null, null, 2, null, null)");

        assertUpdate("DROP TABLE test_nan_partition");
    }

    @Test
    public void testIsNotNullWithNestedData()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "true")
                .build();
        assertUpdate(admin, "create table nest_test(id int, a row(x varchar, y integer, z varchar), b varchar) WITH (format='PARQUET')");
        assertUpdate(admin, "insert into nest_test values(0, null, '1')", 1);
        assertUpdate(admin, "insert into nest_test values(1, ('a', null, 'b'), '1')", 1);
        assertUpdate(admin, "insert into nest_test values(2, ('b', 1, 'd'), '1')", 1);
        assertQuery(admin, "select a.y from nest_test", "values (null), (null), (1)");
        assertQuery(admin, "select id from nest_test where a.y IS NOT NULL", "values (2)");
        assertUpdate(admin, "DROP TABLE nest_test");
    }

    @Test
    public void testSchemaOperations()
    {
        Session session = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        assertUpdate(session, "CREATE SCHEMA new_schema");

        assertUpdate(session, "CREATE TABLE new_schema.test (x bigint)");

        assertQueryFails(session, "DROP SCHEMA new_schema", ".*Cannot drop non-empty schema 'new_schema'");

        assertUpdate(session, "DROP TABLE new_schema.test");

        assertUpdate(session, "DROP SCHEMA new_schema");
    }

    @Test
    public void testSchemaAuthorizationForUser()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        assertUpdate(admin, "CREATE SCHEMA test_schema_authorization_user");

        Session user = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_schema_authorization_user")
                .setIdentity(Identity.forUser("user")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        Session anotherUser = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_schema_authorization_user")
                .setIdentity(Identity.forUser("anotheruser")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        // ordinary users cannot drop a schema or create a table in a schema the do not own
        assertQueryFails(user, "DROP SCHEMA test_schema_authorization_user", "Access Denied: Cannot drop schema test_schema_authorization_user");
        assertQueryFails(user, "CREATE TABLE test_schema_authorization_user.test (x bigint)", "Access Denied: Cannot create table test_schema_authorization_user.test");

        // change owner to user
        assertUpdate(admin, "ALTER SCHEMA test_schema_authorization_user SET AUTHORIZATION user");

        // another user still cannot create tables
        assertQueryFails(anotherUser, "CREATE TABLE test_schema_authorization_user.test (x bigint)", "Access Denied: Cannot create table test_schema_authorization_user.test");

        assertUpdate(user, "CREATE TABLE test_schema_authorization_user.test (x bigint)");

        // another user should not be able to drop the table
        assertQueryFails(anotherUser, "DROP TABLE test_schema_authorization_user.test", "Access Denied: Cannot drop table test_schema_authorization_user.test");
        // or access the table in any way
        assertQueryFails(anotherUser, "SELECT 1 FROM test_schema_authorization_user.test", "Access Denied: Cannot select from table test_schema_authorization_user.test");

        assertUpdate(user, "DROP TABLE test_schema_authorization_user.test");
        assertUpdate(user, "DROP SCHEMA test_schema_authorization_user");
    }

    @Test
    public void testSchemaAuthorizationForRole()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        assertUpdate(admin, "CREATE SCHEMA test_schema_authorization_role");

        // make sure role-grants only work on existing roles
        assertQueryFails(admin, "ALTER SCHEMA test_schema_authorization_role SET AUTHORIZATION ROLE nonexisting_role", ".*?Role 'nonexisting_role' does not exist in catalog 'hive'");

        assertUpdate(admin, "CREATE ROLE authorized_users IN hive");
        assertUpdate(admin, "GRANT authorized_users TO user IN hive");

        assertUpdate(admin, "ALTER SCHEMA test_schema_authorization_role SET AUTHORIZATION ROLE authorized_users");

        Session user = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_schema_authorization_role")
                .setIdentity(Identity.forUser("user")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        Session anotherUser = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_schema_authorization_role")
                .setIdentity(Identity.forUser("anotheruser")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        assertUpdate(user, "CREATE TABLE test_schema_authorization_role.test (x bigint)");

        // another user should not be able to drop the table
        assertQueryFails(anotherUser, "DROP TABLE test_schema_authorization_role.test", "Access Denied: Cannot drop table test_schema_authorization_role.test");
        // or access the table in any way
        assertQueryFails(anotherUser, "SELECT 1 FROM test_schema_authorization_role.test", "Access Denied: Cannot select from table test_schema_authorization_role.test");

        assertUpdate(user, "DROP TABLE test_schema_authorization_role.test");
        assertUpdate(user, "DROP SCHEMA test_schema_authorization_role");

        assertUpdate(admin, "DROP ROLE authorized_users IN hive");
    }

    @Test
    public void testCreateSchemaWithAuthorizationForUser()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        Session user = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_createschema_authorization_user")
                .setIdentity(Identity.forUser("user")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        Session anotherUser = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_createschema_authorization_user")
                .setIdentity(Identity.forUser("anotheruser")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        assertUpdate(admin, "CREATE SCHEMA test_createschema_authorization_user AUTHORIZATION user");
        assertUpdate(user, "CREATE TABLE test_createschema_authorization_user.test (x bigint)");

        // another user should not be able to drop the table
        assertQueryFails(anotherUser, "DROP TABLE test_createschema_authorization_user.test", "Access Denied: Cannot drop table test_createschema_authorization_user.test");
        // or access the table in any way
        assertQueryFails(anotherUser, "SELECT 1 FROM test_createschema_authorization_user.test", "Access Denied: Cannot select from table test_createschema_authorization_user.test");

        assertUpdate(user, "DROP TABLE test_createschema_authorization_user.test");
        assertUpdate(user, "DROP SCHEMA test_createschema_authorization_user");
    }

    @Test
    public void testCreateSchemaWithAuthorizationForRole()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        Session user = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_createschema_authorization_role")
                .setIdentity(Identity.forUser("user")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        Session userWithoutRole = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_createschema_authorization_role")
                .setIdentity(Identity.forUser("user")
                        .withConnectorRoles(Collections.emptyMap())
                        .build())
                .build();

        Session anotherUser = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_createschema_authorization_role")
                .setIdentity(Identity.forUser("anotheruser")
                        .withPrincipal(getSession().getIdentity().getPrincipal())
                        .build())
                .build();

        assertUpdate(admin, "CREATE ROLE authorized_users IN hive");
        assertUpdate(admin, "GRANT authorized_users TO user IN hive");

        assertQueryFails(admin, "CREATE SCHEMA test_createschema_authorization_role AUTHORIZATION ROLE nonexisting_role", ".*?Role 'nonexisting_role' does not exist in catalog 'hive'");
        assertUpdate(admin, "CREATE SCHEMA test_createschema_authorization_role AUTHORIZATION ROLE authorized_users");
        assertUpdate(user, "CREATE TABLE test_createschema_authorization_role.test (x bigint)");

        // "user" without the role enabled cannot create new tables
        assertQueryFails(userWithoutRole, "CREATE TABLE test_schema_authorization_role.test1 (x bigint)", "Access Denied: Cannot create table test_schema_authorization_role.test1");

        // another user should not be able to drop the table
        assertQueryFails(anotherUser, "DROP TABLE test_createschema_authorization_role.test", "Access Denied: Cannot drop table test_createschema_authorization_role.test");
        // or access the table in any way
        assertQueryFails(anotherUser, "SELECT 1 FROM test_createschema_authorization_role.test", "Access Denied: Cannot select from table test_createschema_authorization_role.test");

        assertUpdate(user, "DROP TABLE test_createschema_authorization_role.test");
        assertUpdate(user, "DROP SCHEMA test_createschema_authorization_role");

        assertUpdate(admin, "DROP ROLE authorized_users IN hive");
    }

    @Test
    public void testSchemaAuthorization()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        Session user = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_schema_authorization")
                .setIdentity(Identity.forUser("user").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();

        assertUpdate(admin, "CREATE SCHEMA test_schema_authorization");

        assertUpdate(admin, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION user");
        assertUpdate(user, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION ROLE admin");
        assertQueryFails(user, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION ROLE admin", "Access Denied: Cannot set authorization for schema test_schema_authorization to ROLE admin");

        // switch owner back to user, and then change the owner to ROLE admin from a different catalog to verify roles are relative to the catalog of the schema
        assertUpdate(admin, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION user");
        Session userSessionInDifferentCatalog = testSessionBuilder()
                .setIdentity(Identity.forUser("user").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();
        assertUpdate(userSessionInDifferentCatalog, "ALTER SCHEMA hive.test_schema_authorization SET AUTHORIZATION ROLE admin");
        assertUpdate(admin, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION user");

        assertUpdate(admin, "DROP SCHEMA test_schema_authorization");
    }

    @Test
    public void testTableAuthorization()
    {
        Session admin = Session.builder(getSession())
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("hive").withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin"))).build())
                .build();

        Session alice = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("alice").build())
                .build();

        assertUpdate(admin, "CREATE SCHEMA test_table_authorization");
        assertUpdate(admin, "CREATE TABLE test_table_authorization.foo (col int)");

        assertAccessDenied(
                alice,
                "ALTER TABLE test_table_authorization.foo SET AUTHORIZATION alice",
                "Cannot set authorization for table test_table_authorization.foo to USER alice");
        assertUpdate(admin, "ALTER TABLE test_table_authorization.foo SET AUTHORIZATION alice");
        assertUpdate(alice, "ALTER TABLE test_table_authorization.foo SET AUTHORIZATION admin");

        assertUpdate(admin, "DROP TABLE test_table_authorization.foo");
        assertUpdate(admin, "DROP SCHEMA test_table_authorization");
    }

    @Test
    public void testTableAuthorizationForRole()
    {
        Session admin = Session.builder(getSession())
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("hive").withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin"))).build())
                .build();

        Session alice = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("alice").build())
                .build();

        assertUpdate(admin, "CREATE SCHEMA test_table_authorization");
        assertUpdate(admin, "CREATE TABLE test_table_authorization.foo (col int)");

        // TODO Change assertions once https://github.com/trinodb/trino/issues/5706 is done
        assertAccessDenied(
                alice,
                "ALTER TABLE test_table_authorization.foo SET AUTHORIZATION ROLE admin",
                "Cannot set authorization for table test_table_authorization.foo to ROLE admin");
        assertUpdate(admin, "ALTER TABLE test_table_authorization.foo SET AUTHORIZATION alice");
        assertQueryFails(
                alice,
                "ALTER TABLE test_table_authorization.foo SET AUTHORIZATION ROLE admin",
                "Setting table owner type as a role is not supported");

        assertUpdate(admin, "DROP TABLE test_table_authorization.foo");
        assertUpdate(admin, "DROP SCHEMA test_table_authorization");
    }

    @Test
    public void testViewAuthorization()
    {
        Session admin = Session.builder(getSession())
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("hive").withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin"))).build())
                .build();

        Session alice = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("alice").build())
                .build();

        String schema = "test_view_authorization" + TestTable.randomTableSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view AS SELECT current_user AS user");

        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice",
                "Cannot set authorization for view " + schema + ".test_view to USER alice");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        assertUpdate(alice, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION admin");

        assertUpdate(admin, "DROP VIEW " + schema + ".test_view");
        assertUpdate(admin, "DROP SCHEMA " + schema);
    }

    @Test
    public void testViewAuthorizationSecurityDefiner()
    {
        Session admin = Session.builder(getSession())
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("hive").withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin"))).build())
                .build();

        Session alice = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("alice").build())
                .build();

        String schema = "test_view_authorization" + TestTable.randomTableSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE TABLE " + schema + ".test_table (col int)");
        assertUpdate(admin, "INSERT INTO " + schema + ".test_table VALUES (1)", 1);
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view SECURITY DEFINER AS SELECT * from " + schema + ".test_table");
        assertUpdate(admin, "GRANT SELECT ON " + schema + ".test_view TO alice");

        assertQuery(alice, "SELECT * FROM " + schema + ".test_view", "VALUES (1)");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        assertQueryFails(alice, "SELECT * FROM " + schema + ".test_view", "Access Denied: Cannot select from table " + schema + ".test_table");

        assertUpdate(alice, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION admin");
        assertUpdate(admin, "DROP VIEW " + schema + ".test_view");
        assertUpdate(admin, "DROP TABLE " + schema + ".test_table");
        assertUpdate(admin, "DROP SCHEMA " + schema);
    }

    @Test
    public void testViewAuthorizationSecurityInvoker()
    {
        Session admin = Session.builder(getSession())
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("hive").withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin"))).build())
                .build();

        Session alice = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("alice").build())
                .build();

        String schema = "test_view_authorization" + TestTable.randomTableSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE TABLE " + schema + ".test_table (col int)");
        assertUpdate(admin, "INSERT INTO " + schema + ".test_table VALUES (1)", 1);
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view SECURITY INVOKER AS SELECT * from " + schema + ".test_table");
        assertUpdate(admin, "GRANT SELECT ON " + schema + ".test_view TO alice");

        assertQueryFails(alice, "SELECT * FROM " + schema + ".test_view", "Access Denied: Cannot select from table " + schema + ".test_table");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        assertQueryFails(alice, "SELECT * FROM " + schema + ".test_view", "Access Denied: Cannot select from table " + schema + ".test_table");

        assertUpdate(alice, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION admin");
        assertUpdate(admin, "DROP VIEW " + schema + ".test_view");
        assertUpdate(admin, "DROP TABLE " + schema + ".test_table");
        assertUpdate(admin, "DROP SCHEMA " + schema);
    }

    @Test
    public void testViewAuthorizationForRole()
    {
        Session admin = Session.builder(getSession())
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("hive").withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin"))).build())
                .build();

        Session alice = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("alice").build())
                .build();

        String schema = "test_view_authorization" + TestTable.randomTableSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE TABLE " + schema + ".test_table (col int)");
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view AS SELECT * FROM " + schema + ".test_table");

        // TODO Change assertions once https://github.com/trinodb/trino/issues/5706 is done
        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION ROLE admin",
                "Cannot set authorization for view " + schema + ".test_view to ROLE admin");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        assertQueryFails(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION ROLE admin",
                "Setting table owner type as a role is not supported");

        assertUpdate(admin, "DROP VIEW " + schema + ".test_view");
        assertUpdate(admin, "DROP TABLE " + schema + ".test_table");
        assertUpdate(admin, "DROP SCHEMA " + schema);
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        Session user = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema("test_show_create_schema")
                .setIdentity(Identity.forUser("user").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();

        assertUpdate(admin, "CREATE ROLE test_show_create_schema_role IN hive");
        assertUpdate(admin, "GRANT test_show_create_schema_role TO user IN hive");

        assertUpdate(admin, "CREATE SCHEMA test_show_create_schema");

        String createSchemaSql = format("" +
                        "CREATE SCHEMA %s.test_show_create_schema\n" +
                        "AUTHORIZATION USER hive\n" +
                        "WITH \\(\n" +
                        "   location = '.*test_show_create_schema'\n" +
                        "\\)",
                getSession().getCatalog().get());

        String actualResult = getOnlyElement(computeActual(admin, "SHOW CREATE SCHEMA test_show_create_schema").getOnlyColumnAsSet()).toString();
        assertThat(actualResult).matches(createSchemaSql);

        assertQueryFails(user, "SHOW CREATE SCHEMA test_show_create_schema", "Access Denied: Cannot show create schema for test_show_create_schema");

        assertUpdate(admin, "ALTER SCHEMA test_show_create_schema SET AUTHORIZATION ROLE test_show_create_schema_role");

        createSchemaSql = format("" +
                        "CREATE SCHEMA %s.test_show_create_schema\n" +
                        "AUTHORIZATION ROLE test_show_create_schema_role\n" +
                        "WITH \\(\n" +
                        "   location = '.*test_show_create_schema'\n" +
                        "\\)",
                getSession().getCatalog().get());

        actualResult = getOnlyElement(computeActual(admin, "SHOW CREATE SCHEMA test_show_create_schema").getOnlyColumnAsSet()).toString();
        assertThat(actualResult).matches(createSchemaSql);

        assertUpdate(user, "DROP SCHEMA test_show_create_schema");
        assertUpdate(admin, "DROP ROLE test_show_create_schema_role IN hive");
    }

    @Test
    public void testIoExplain()
    {
        // Test IO explain with small number of discrete components.
        computeActual("CREATE TABLE test_io_explain WITH (partitioned_by = ARRAY['orderkey', 'processing']) AS SELECT custkey, orderkey, orderstatus = 'P' processing FROM orders WHERE orderkey < 3");

        EstimatedStatsAndCost estimate = new EstimatedStatsAndCost(2.0, 40.0, 40.0, 0.0, 0.0);
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_io_explain SELECT custkey, orderkey, processing FROM test_io_explain WHERE custkey <= 10");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_io_explain"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY)),
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "processing",
                                                        BOOLEAN,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("false"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("false"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "custkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.empty(), ABOVE),
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY)))))),
                                        estimate)),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_io_explain")),
                        estimate));

        assertUpdate("DROP TABLE test_io_explain");

        // Test IO explain with large number of discrete components where Domain::simpify comes into play.
        computeActual("CREATE TABLE test_io_explain WITH (partitioned_by = ARRAY['orderkey']) AS SELECT custkey, orderkey FROM orders WHERE orderkey < 200");

        estimate = new EstimatedStatsAndCost(55.0, 990.0, 990.0, 0.0, 0.0);
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_io_explain SELECT custkey, orderkey + 10 FROM test_io_explain WHERE custkey <= 10");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_io_explain"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("199"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "custkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.empty(), ABOVE),
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY)))))),
                                        estimate)),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_io_explain")),
                        estimate));

        EstimatedStatsAndCost finalEstimate = new EstimatedStatsAndCost(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        estimate = new EstimatedStatsAndCost(1.0, 18.0, 18, 0.0, 0.0);
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_io_explain SELECT custkey, orderkey FROM test_io_explain WHERE orderkey = 100");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_io_explain"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("100"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("100"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("100"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("100"), EXACTLY)))))),
                                        estimate)),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_io_explain")),
                        finalEstimate));

        assertUpdate("DROP TABLE test_io_explain");
    }

    @Test
    public void testIoExplainColumnFilters()
    {
        // Test IO explain with small number of discrete components.
        computeActual("CREATE TABLE test_io_explain_column_filters WITH (partitioned_by = ARRAY['orderkey']) AS SELECT custkey, orderstatus, orderkey FROM orders WHERE orderkey < 3");

        EstimatedStatsAndCost estimate = new EstimatedStatsAndCost(2.0, 48.0, 48.0, 0.0, 0.0);
        EstimatedStatsAndCost finalEstimate = new EstimatedStatsAndCost(0.0, 0.0, 96.0, 0.0, 0.0);
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT custkey, orderkey, orderstatus FROM test_io_explain_column_filters WHERE custkey <= 10 and orderstatus='P'");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_io_explain_column_filters"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY)),
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "custkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.empty(), ABOVE),
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "orderstatus",
                                                        VarcharType.createVarcharType(1),
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("P"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("P"), EXACTLY)))))),
                                        estimate)),
                        Optional.empty(),
                        finalEstimate));
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT custkey, orderkey, orderstatus FROM test_io_explain_column_filters WHERE custkey <= 10 and (orderstatus='P' or orderstatus='S')");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_io_explain_column_filters"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY)),
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "orderstatus",
                                                        VarcharType.createVarcharType(1),
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("P"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("P"), EXACTLY)),
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("S"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("S"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "custkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.empty(), ABOVE),
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY)))))),
                                        estimate)),
                        Optional.empty(),
                        finalEstimate));
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT custkey, orderkey, orderstatus FROM test_io_explain_column_filters WHERE custkey <= 10 and cast(orderstatus as integer) = 5");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "test_io_explain_column_filters"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("1"), EXACTLY)),
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("2"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "custkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.empty(), ABOVE),
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY)))))),
                                        estimate)),
                        Optional.empty(),
                        finalEstimate));

        assertUpdate("DROP TABLE test_io_explain_column_filters");
    }

    @Test
    public void testIoExplainNoFilter()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        assertUpdate(
                admin,
                "create table io_explain_test_no_filter(\n"
                        + "id integer,\n"
                        + "a varchar,\n"
                        + "b varchar,\n"
                        + "ds varchar)"
                        + "WITH (format='PARQUET', partitioned_by = ARRAY['ds'])");
        assertUpdate(admin, "insert into io_explain_test_no_filter(id,a,ds) values(1, 'a','a')", 1);

        EstimatedStatsAndCost estimate = new EstimatedStatsAndCost(1.0, 22.0, 22.0, 0.0, 0.0);
        EstimatedStatsAndCost finalEstimate = new EstimatedStatsAndCost(1.0, 22.0, 22.0, 0.0, 22.0);
        MaterializedResult result =
                computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM io_explain_test_no_filter");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "io_explain_test_no_filter"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "ds",
                                                        VARCHAR,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("a"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("a"), EXACTLY)))))),
                                        estimate)),
                        Optional.empty(),
                        finalEstimate));
        assertUpdate("DROP TABLE io_explain_test_no_filter");
    }

    @Test
    public void testIoExplainFilterOnAgg()
    {
        Session admin = Session.builder(getSession())
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        assertUpdate(
                admin,
                "create table io_explain_test_filter_on_agg(\n"
                        + "id integer,\n"
                        + "a varchar,\n"
                        + "b varchar,\n"
                        + "ds varchar)"
                        + "WITH (format='PARQUET', partitioned_by = ARRAY['ds'])");
        assertUpdate(admin, "insert into io_explain_test_filter_on_agg(id,a,ds) values(1, 'a','a')", 1);

        EstimatedStatsAndCost estimate = new EstimatedStatsAndCost(1.0, 5.0, 5.0, 0.0, 0.0);
        EstimatedStatsAndCost finalEstimate = new EstimatedStatsAndCost(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        MaterializedResult result =
                computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM (SELECT COUNT(*) cnt FROM io_explain_test_filter_on_agg WHERE b = 'b') WHERE cnt > 0");
        assertEquals(
                getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlan(
                        ImmutableSet.of(
                                new TableColumnInfo(
                                        new CatalogSchemaTableName(catalog, "tpch", "io_explain_test_filter_on_agg"),
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "ds",
                                                        VARCHAR,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("a"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("a"), EXACTLY))))),
                                                new ColumnConstraint(
                                                        "b",
                                                        VARCHAR,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("b"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("b"), EXACTLY)))))),
                                        estimate)),
                        Optional.empty(),
                        finalEstimate));
        assertUpdate("DROP TABLE io_explain_test_filter_on_agg");
    }

    @Test
    public void testIoExplainWithPrimitiveTypes()
    {
        // Use LinkedHashMap to maintain insertion order for ease of locating
        // map entry if assertion in the loop below fails.
        Map<Object, TypeAndEstimate> data = new LinkedHashMap<>();
        data.put("foo", new TypeAndEstimate(createUnboundedVarcharType(), new EstimatedStatsAndCost(1.0, 16.0, 16.0, 0.0, 0.0)));
        data.put(Byte.toString((byte) (Byte.MAX_VALUE / 2)), new TypeAndEstimate(TINYINT, new EstimatedStatsAndCost(1.0, 10.0, 10.0, 0.0, 0.0)));
        data.put(Short.toString((short) (Short.MAX_VALUE / 2)), new TypeAndEstimate(SMALLINT, new EstimatedStatsAndCost(1.0, 11.0, 11.0, 0.0, 0.0)));
        data.put(Integer.toString(Integer.MAX_VALUE / 2), new TypeAndEstimate(INTEGER, new EstimatedStatsAndCost(1.0, 13.0, 13.0, 0.0, 0.0)));
        data.put(Long.toString(Long.MAX_VALUE / 2), new TypeAndEstimate(BIGINT, new EstimatedStatsAndCost(1.0, 17.0, 17.0, 0.0, 0.0)));
        data.put(Boolean.TRUE.toString(), new TypeAndEstimate(BOOLEAN, new EstimatedStatsAndCost(1.0, 10.0, 10.0, 0.0, 0.0)));
        data.put("bar", new TypeAndEstimate(createCharType(3), new EstimatedStatsAndCost(1.0, 16.0, 16.0, 0.0, 0.0)));
        data.put("1.2345678901234578E14", new TypeAndEstimate(DOUBLE, new EstimatedStatsAndCost(1.0, 17.0, 17.0, 0.0, 0.0)));
        data.put("123456789012345678901234.567", new TypeAndEstimate(createDecimalType(30, 3), new EstimatedStatsAndCost(1.0, 25.0, 25.0, 0.0, 0.0)));
        data.put("2019-01-01", new TypeAndEstimate(DateType.DATE, new EstimatedStatsAndCost(1.0, 13.0, 13.0, 0.0, 0.0)));
        data.put("2019-01-01 23:22:21.123", new TypeAndEstimate(TimestampType.TIMESTAMP_MILLIS, new EstimatedStatsAndCost(1.0, 17.0, 17.0, 0.0, 0.0)));
        int index = 0;
        for (Map.Entry<Object, TypeAndEstimate> entry : data.entrySet()) {
            index++;
            Type type = entry.getValue().type;
            EstimatedStatsAndCost estimate = entry.getValue().estimate;
            @Language("SQL") String query = format(
                    "CREATE TABLE test_types_table  WITH (partitioned_by = ARRAY['my_col']) AS " +
                            "SELECT 'foo' my_non_partition_col, CAST('%s' AS %s) my_col",
                    entry.getKey(),
                    type.getDisplayName());

            assertUpdate(query, 1);
            assertEquals(
                    getIoPlanCodec().fromJson((String) getOnlyElement(computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM test_types_table").getOnlyColumnAsSet())),
                    new IoPlan(
                            ImmutableSet.of(new TableColumnInfo(
                                    new CatalogSchemaTableName(catalog, "tpch", "test_types_table"),
                                    ImmutableSet.of(
                                            new ColumnConstraint(
                                                    "my_col",
                                                    type,
                                                    new FormattedDomain(
                                                            false,
                                                            ImmutableSet.of(
                                                                    new FormattedRange(
                                                                            new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY),
                                                                            new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY)))))),
                                    estimate)),
                            Optional.empty(),
                            estimate),
                    format("%d) Type %s ", index, type));

            assertUpdate("DROP TABLE test_types_table");
        }
    }

    @Test
    public void testReadNoColumns()
    {
        testWithAllStorageFormats(this::testReadNoColumns);
    }

    private void testReadNoColumns(Session session, HiveStorageFormat storageFormat)
    {
        assertUpdate(session, format("CREATE TABLE test_read_no_columns WITH (format = '%s') AS SELECT 0 x", storageFormat), 1);
        assertQuery(session, "SELECT count(*) FROM test_read_no_columns", "SELECT 1");
        assertUpdate(session, "DROP TABLE test_read_no_columns");
    }

    @Test
    public void createTableWithEveryType()
    {
        @Language("SQL") String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 2 _integer" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('bar' AS CHAR(10)) _char";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(7), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTable);
    }

    private void testCreatePartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ",  _varchar VARCHAR(65535)" +
                ", _char CHAR(10)" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _smallint SMALLINT" +
                ", _tinyint TINYINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _partition_string VARCHAR" +
                ", _partition_varchar VARCHAR(65535)" +
                ", _partition_char CHAR(10)" +
                ", _partition_tinyint TINYINT" +
                ", _partition_smallint SMALLINT" +
                ", _partition_integer INTEGER" +
                ", _partition_bigint BIGINT" +
                ", _partition_boolean BOOLEAN" +
                ", _partition_decimal_short DECIMAL(3,2)" +
                ", _partition_decimal_long DECIMAL(30,10)" +
                ", _partition_date DATE" +
                ", _partition_timestamp TIMESTAMP" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_string', '_partition_varchar', '_partition_char', '_partition_tinyint', '_partition_smallint', '_partition_integer', '_partition_bigint', '_partition_boolean', '_partition_decimal_short', '_partition_decimal_long', '_partition_date', '_partition_timestamp']" +
                ") ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of(
                "_partition_string",
                "_partition_varchar",
                "_partition_char",
                "_partition_tinyint",
                "_partition_smallint",
                "_partition_integer",
                "_partition_bigint",
                "_partition_boolean",
                "_partition_decimal_short",
                "_partition_decimal_long",
                "_partition_date",
                "_partition_timestamp");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
        }

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));
        assertColumnType(tableMetadata, "_partition_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_partition_varchar", createVarcharType(65535));

        MaterializedResult result = computeActual("SELECT * FROM test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", CAST(1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", 'foo' _partition_string" +
                ", 'bar' _partition_varchar" +
                ", CAST('boo' AS CHAR(10)) _partition_char" +
                ", CAST(1 AS TINYINT) _partition_tinyint" +
                ", CAST(1 AS SMALLINT) _partition_smallint" +
                ", 1 _partition_integer" +
                ", CAST (1 AS BIGINT) _partition_bigint" +
                ", true _partition_boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _partition_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _partition_decimal_long" +
                ", CAST('2017-05-01' AS DATE) _partition_date" +
                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _partition_timestamp";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * FROM test_partitioned_table", select);
        assertQuery(session,
                "SELECT * FROM test_partitioned_table WHERE" +
                        " 'foo' = _partition_string" +
                        " AND 'bar' = _partition_varchar" +
                        " AND CAST('boo' AS CHAR(10)) = _partition_char" +
                        " AND CAST(1 AS TINYINT) = _partition_tinyint" +
                        " AND CAST(1 AS SMALLINT) = _partition_smallint" +
                        " AND 1 = _partition_integer" +
                        " AND CAST(1 AS BIGINT) = _partition_bigint" +
                        " AND true = _partition_boolean" +
                        " AND CAST('3.14' AS DECIMAL(3,2)) = _partition_decimal_short" +
                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _partition_decimal_long" +
                        " AND CAST('2017-05-01' AS DATE) = _partition_date" +
                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _partition_timestamp",
                select);

        assertUpdate(session, "DROP TABLE test_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_partitioned_table"));
    }

    @Test
    public void createTableLike()
    {
        createTableLike("", false);
        createTableLike("EXCLUDING PROPERTIES", false);
        createTableLike("INCLUDING PROPERTIES", true);
    }

    private void createTableLike(String likeSuffix, boolean hasPartition)
    {
        // Create a non-partitioned table
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_table_original (" +
                "  tinyint_col tinyint " +
                ", smallint_col smallint" +
                ")";
        assertUpdate(createTable);

        // Verify the table is correctly created
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_table_original");
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);

        // Create a partitioned table
        @Language("SQL") String createPartitionedTable = "" +
                "CREATE TABLE test_partitioned_table_original (" +
                "  string_col VARCHAR" +
                ", decimal_long_col DECIMAL(30,10)" +
                ", partition_bigint BIGINT" +
                ", partition_decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY['partition_bigint', 'partition_decimal_long']" +
                ")";
        assertUpdate(createPartitionedTable);

        // Verify the table is correctly created
        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_original");

        // Verify the partition keys are correctly created
        List<String> partitionedBy = ImmutableList.of("partition_bigint", "partition_decimal_long");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        // Create a table using only one LIKE
        @Language("SQL") String createTableSingleLike = "" +
                "CREATE TABLE test_partitioned_table_single_like (" +
                "LIKE test_partitioned_table_original " + likeSuffix +
                ")";
        assertUpdate(createTableSingleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_single_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableLikeExtra = "" +
                "CREATE TABLE test_partitioned_table_like_extra (" +
                "  bigint_col BIGINT" +
                ", double_col DOUBLE" +
                ", LIKE test_partitioned_table_single_like " + likeSuffix +
                ")";
        assertUpdate(createTableLikeExtra);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_like_extra");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "bigint_col", BIGINT);
        assertColumnType(tableMetadata, "double_col", DOUBLE);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableDoubleLike = "" +
                "CREATE TABLE test_partitioned_table_double_like (" +
                "  LIKE test_table_original " +
                ", LIKE test_partitioned_table_like_extra " + likeSuffix +
                ")";
        assertUpdate(createTableDoubleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_double_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        assertUpdate("DROP TABLE test_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_single_like");
        assertUpdate("DROP TABLE test_partitioned_table_like_extra");
        assertUpdate("DROP TABLE test_partitioned_table_double_like");
    }

    @Test
    public void testCreateTableAs()
    {
        testWithAllStorageFormats(this::testCreateTableAs);
    }

    private void testCreateTableAs(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", CAST('bar' AS CHAR(10)) _char" +
                ", CAST (1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST ('123.45' as REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        String createTableAs = format("CREATE TABLE test_format_table WITH (format = '%s') AS %s", storageFormat, select);

        assertUpdate(session, createTableAs, 1);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_varchar", createVarcharType(3));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        // assure reader supports basic column reordering and pruning
        assertQuery(session, "SELECT _integer, _varchar, _integer FROM test_format_table", "SELECT 2, 'foo', 2");

        assertQuery(session, "SELECT * FROM test_format_table", select);

        assertUpdate(session, "DROP TABLE test_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_format_table"));
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTableAs);
    }

    private void testCreatePartitionedTableAs(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) FROM orders");

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * FROM test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as");

        assertFalse(getQueryRunner().tableExists(session, "test_create_partitioned_table_as"));
    }

    @Test
    public void testCreateTableWithUnsupportedType()
    {
        assertQueryFails("CREATE TABLE test_create_table_with_unsupported_type(x time)", "\\QUnsupported Hive type: time(3)\\E");
        assertQueryFails("CREATE TABLE test_create_table_with_unsupported_type AS SELECT TIME '00:00:00' x", "\\QUnsupported Hive type: time(0)\\E");
    }

    @Test
    public void testTargetMaxFileSize()
    {
        testTargetMaxFileSize(3);
    }

    protected void testTargetMaxFileSize(int expectedTableWriters)
    {
        // We use TEXTFILE in this test because is has a very consistent and predictable size
        @Language("SQL") String createTableSql = "CREATE TABLE test_max_file_size WITH (format = 'TEXTFILE') AS SELECT * FROM tpch.sf1.lineitem LIMIT 1000000";
        @Language("SQL") String selectFileInfo = "SELECT distinct \"$path\", \"$file_size\" FROM test_max_file_size";

        // verify the default behavior is one file per node
        Session session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                .build();
        assertUpdate(session, createTableSql, 1000000);
        assertThat(computeActual(selectFileInfo).getRowCount()).isEqualTo(expectedTableWriters);
        assertUpdate("DROP TABLE test_max_file_size");

        // Write table with small limit and verify we get multiple files per node near the expected size
        // Writer writes chunks of rows that are about 1MB
        DataSize maxSize = DataSize.of(1, Unit.MEGABYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                .setCatalogSessionProperty("hive", "target_max_file_size", maxSize.toString())
                .build();

        assertUpdate(session, createTableSql, 1000000);
        MaterializedResult result = computeActual(selectFileInfo);
        assertThat(result.getRowCount()).isGreaterThan(expectedTableWriters);
        for (MaterializedRow row : result) {
            // allow up to a larger delta due to the very small max size and the relatively large writer chunk size
            assertThat((Long) row.getField(1)).isLessThan(maxSize.toBytes() * 3);
        }

        assertUpdate("DROP TABLE test_max_file_size");
    }

    @Test
    public void testTargetMaxFileSizePartitioned()
    {
        testTargetMaxFileSizePartitioned(3);
    }

    protected void testTargetMaxFileSizePartitioned(int expectedTableWriters)
    {
        // We use TEXTFILE in this test because is has a very consistent and predictable size
        @Language("SQL") String createTableSql = "" +
                "CREATE TABLE test_max_file_size_partitioned WITH (partitioned_by = ARRAY['returnflag'], format = 'TEXTFILE') AS " +
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.sf1.lineitem LIMIT 1000000";
        @Language("SQL") String selectFileInfo = "SELECT distinct \"$path\", \"$file_size\" FROM test_max_file_size_partitioned";

        // verify the default behavior is one file per node per partition
        Session session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                .build();
        assertUpdate(session, createTableSql, 1000000);
        assertThat(computeActual(selectFileInfo).getRowCount()).isEqualTo(expectedTableWriters * 3);
        assertUpdate("DROP TABLE test_max_file_size_partitioned");

        // Write table with small limit and verify we get multiple files per node near the expected size
        // Writer writes chunks of rows that are about 1MB
        DataSize maxSize = DataSize.of(1, Unit.MEGABYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                .setCatalogSessionProperty("hive", "target_max_file_size", maxSize.toString())
                .build();

        assertUpdate(session, createTableSql, 1000000);
        MaterializedResult result = computeActual(selectFileInfo);
        assertThat(result.getRowCount()).isGreaterThan(expectedTableWriters * 3);
        for (MaterializedRow row : result) {
            // allow up to a larger delta due to the very small max size and the relatively large writer chunk size
            assertThat((Long) row.getField(1)).isLessThan(maxSize.toBytes() * 3);
        }

        assertUpdate("DROP TABLE test_max_file_size_partitioned");
    }

    @Test
    public void testPropertiesTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_show_properties" +
                " WITH (" +
                "format = 'orc', " +
                "partitioned_by = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_columns = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_fpp = 0.5" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        String queryId = (String) computeScalar("SELECT query_id FROM system.runtime.queries WHERE query LIKE 'CREATE TABLE test_show_properties%'");
        String nodeVersion = (String) computeScalar("SELECT node_version FROM system.runtime.nodes WHERE coordinator");
        assertQuery("SELECT \"orc.bloom.filter.columns\", \"orc.bloom.filter.fpp\", presto_query_id, presto_version, transactional FROM \"test_show_properties$properties\"",
                format("SELECT 'ship_priority,order_status', '0.5', '%s', '%s', 'false'", queryId, nodeVersion));
        assertUpdate("DROP TABLE test_show_properties");
    }

    @Test
    public void testCreatePartitionedTableInvalidColumnOrdering()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("" +
                "CREATE TABLE test_create_table_invalid_column_ordering\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['apple'])"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Partition keys must be the last columns in the table and in the same order as the table properties.*");
    }

    @Test
    public void testCreatePartitionedTableAsInvalidColumnOrdering()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("" +
                "CREATE TABLE test_create_table_as_invalid_column_ordering " +
                "WITH (partitioned_by = ARRAY['SHIP_PRIORITY', 'ORDER_STATUS']) " +
                "AS " +
                "SELECT shippriority AS ship_priority, orderkey AS order_key, orderstatus AS order_status " +
                "FROM tpch.tiny.orders"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Partition keys must be the last columns in the table and in the same order as the table properties.*");
    }

    @Test
    public void testCreateTableOnlyPartitionColumns()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("" +
                "CREATE TABLE test_create_table_only_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['grape', 'apple', 'orange', 'pear'])"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Table contains only partition columns");
    }

    @Test
    public void testCreateTableNonExistentPartitionColumns()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("" +
                "CREATE TABLE test_create_table_nonexistent_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['dragonfruit'])"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Partition columns .* not present in schema");
    }

    @Test
    public void testCreateTableUnsupportedPartitionType()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("" +
                "CREATE TABLE test_create_table_unsupported_partition_type " +
                "(foo bigint, bar ARRAY(varchar)) " +
                "WITH (partitioned_by = ARRAY['bar'])"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Unsupported type .* for partition: .*");
    }

    @Test
    public void testCreateTableUnsupportedPartitionTypeAs()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("" +
                "CREATE TABLE test_create_table_unsupported_partition_type_as " +
                "WITH (partitioned_by = ARRAY['a']) " +
                "AS " +
                "SELECT 123 x, ARRAY ['foo'] a"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Unsupported type .* for partition: a");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type: varchar\\(65536\\)\\. Supported VARCHAR types: VARCHAR\\(<=65535\\), VARCHAR\\.")
    public void testCreateTableNonSupportedVarcharColumn()
    {
        assertUpdate("CREATE TABLE test_create_table_non_supported_varchar_column (apple varchar(65536))");
    }

    @Test
    public void testEmptyBucketedTable()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testEmptyBucketedTable);
    }

    private void testEmptyBucketedTable(Session session, HiveStorageFormat storageFormat)
    {
        testEmptyBucketedTable(session, storageFormat, true);
        testEmptyBucketedTable(session, storageFormat, false);
    }

    private void testEmptyBucketedTable(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_empty_bucketed_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(bucket_key VARCHAR, col_1 VARCHAR, col2 VARCHAR) " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") ";

        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertNull(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        assertEquals(computeActual("SELECT * from " + tableName).getRowCount(), 0);

        // make sure that we will get one file per bucket regardless of writer count configured
        Session parallelWriter = Session.builder(getParallelWriteSession())
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c0')", 1);
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a1', 'b1', 'c1')", 1);

        assertQuery("SELECT * from " + tableName, "VALUES ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1')");

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testBucketedTable()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testBucketedTable);
    }

    private void testBucketedTable(Session session, HiveStorageFormat storageFormat)
    {
        testBucketedTable(session, storageFormat, true);
        testBucketedTable(session, storageFormat, false);
    }

    private void testBucketedTable(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_bucketed_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t (bucket_key, col_1, col_2)";

        // make sure that we will get one file per bucket regardless of writer count configured
        Session parallelWriter = Session.builder(getParallelWriteSession())
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(parallelWriter, createTable, 3);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertNull(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        assertQuery("SELECT * from " + tableName, "VALUES ('a', 'b', 'c'), ('aa', 'bb', 'cc'), ('aaa', 'bbb', 'ccc')");

        assertUpdate(
                parallelWriter,
                "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c0')",
                1,
                // buckets should be repartitioned locally hence local repartitioned exchange should exist in plan
                assertLocalRepartitionedExchangesCount(1));
        assertUpdate(parallelWriter, "INSERT INTO " + tableName + " VALUES ('a1', 'b1', 'c1')", 1);

        assertQuery("SELECT * from " + tableName, "VALUES ('a', 'b', 'c'), ('aa', 'bb', 'cc'), ('aaa', 'bbb', 'ccc'), ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1')");

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    /**
     * Regression test for https://github.com/trinodb/trino/issues/5295
     */
    @Test
    public void testBucketedTableWithTimestampColumn()
    {
        String tableName = "test_bucketed_table_with_timestamp_" + randomTableSuffix();

        String createTable = "" +
                "CREATE TABLE " + tableName + " (" +
                "  bucket_key integer, " +
                "  a_timestamp timestamp(3) " +
                ")" +
                "WITH (" +
                "  bucketed_by = ARRAY[ 'bucket_key' ], " +
                "  bucket_count = 11 " +
                ") ";
        assertUpdate(createTable);

        assertQuery(
                "DESCRIBE " + tableName,
                "VALUES " +
                        "('bucket_key', 'integer', '', ''), " +
                        "('a_timestamp', 'timestamp(3)', '', '')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreatePartitionedBucketedTableAsFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testCreatePartitionedBucketedTableAsFewRows);
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat)
    {
        testCreatePartitionedBucketedTableAsFewRows(session, storageFormat, true);
        testCreatePartitionedBucketedTableAsFewRows(session, storageFormat, false);
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_few_rows";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t(bucket_key, col, partition_key)";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                Session.builder(getParallelWriteSession())
                        .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                        .build(),
                createTable,
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAs()
    {
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAs(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_as";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) FROM orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableWithNullsAs()
    {
        testCreatePartitionedBucketedTableWithNullsAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableWithNullsAs(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_with_nulls_as";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderpriority_nulls', 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'orderkey' ], " +
                "bucket_count = 4 " +
                ") " +
                "AS " +
                "SELECT custkey, orderkey, comment, nullif(orderpriority, '1-URGENT') orderpriority_nulls, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) FROM orders");

        // verify that we create bucket_count files in each partition
        assertEqualsIgnoreOrder(
                computeActual(format("SELECT orderpriority_nulls, orderstatus, COUNT(DISTINCT \"$path\") FROM %s GROUP BY 1, 2", tableName)),
                resultBuilder(getSession(), createVarcharType(1), BIGINT)
                        .row(null, "F", 4L)
                        .row(null, "O", 4L)
                        .row(null, "P", 4L)
                        .row("2-HIGH", "F", 4L)
                        .row("2-HIGH", "O", 4L)
                        .row("2-HIGH", "P", 4L)
                        .row("3-MEDIUM", "F", 4L)
                        .row("3-MEDIUM", "O", 4L)
                        .row("3-MEDIUM", "P", 4L)
                        .row("4-NOT SPECIFIED", "F", 4L)
                        .row("4-NOT SPECIFIED", "O", 4L)
                        .row("4-NOT SPECIFIED", "P", 4L)
                        .row("5-LOW", "F", 4L)
                        .row("5-LOW", "O", 4L)
                        .row("5-LOW", "P", 4L)
                        .build());

        assertQuery("SELECT * FROM " + tableName, "SELECT custkey, orderkey, comment, nullif(orderpriority, '1-URGENT') orderpriority_nulls, orderstatus FROM orders");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertIntoPartitionedBucketedTableFromBucketedTable()
    {
        testInsertIntoPartitionedBucketedTableFromBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testInsertIntoPartitionedBucketedTableFromBucketedTable(HiveStorageFormat storageFormat)
    {
        String sourceTable = "test_insert_partitioned_bucketed_table_source";
        String targetTable = "test_insert_partitioned_bucketed_table_target";
        try {
            @Language("SQL") String createSourceTable = "" +
                    "CREATE TABLE " + sourceTable + " " +
                    "WITH (" +
                    "format = '" + storageFormat + "', " +
                    "bucketed_by = ARRAY[ 'custkey' ], " +
                    "bucket_count = 10 " +
                    ") " +
                    "AS " +
                    "SELECT custkey, comment, orderstatus " +
                    "FROM tpch.tiny.orders";
            @Language("SQL") String createTargetTable = "" +
                    "CREATE TABLE " + targetTable + " " +
                    "WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'orderstatus' ], " +
                    "bucketed_by = ARRAY[ 'custkey' ], " +
                    "bucket_count = 10 " +
                    ") " +
                    "AS " +
                    "SELECT custkey, comment, orderstatus " +
                    "FROM tpch.tiny.orders";

            assertUpdate(getParallelWriteSession(), createSourceTable, "SELECT count(*) FROM orders");
            assertUpdate(getParallelWriteSession(), createTargetTable, "SELECT count(*) FROM orders");

            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl()).execute(
                    getParallelWriteSession(),
                    transactionalSession -> {
                        assertUpdate(
                                transactionalSession,
                                "INSERT INTO " + targetTable + " SELECT * FROM " + sourceTable,
                                15000,
                                // there should be two remove exchanges, one below TableWriter and one below TableCommit
                                assertRemoteExchangesCount(transactionalSession, 2));
                    });
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    @Test
    public void testCreatePartitionedBucketedTableAsWithUnionAll()
    {
        testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_with_union_all";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 0 " +
                "UNION ALL " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 1";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) FROM orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void verifyPartitionedBucketedTable(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("orderstatus"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey", "custkey2"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        // verify that we create bucket_count files in each partition
        assertEqualsIgnoreOrder(
                computeActual(format("SELECT orderstatus, COUNT(DISTINCT \"$path\") FROM %s GROUP BY 1", tableName)),
                resultBuilder(getSession(), createVarcharType(1), BIGINT)
                        .row("F", 11L)
                        .row("O", 11L)
                        .row("P", 11L)
                        .build());

        assertQuery("SELECT * FROM " + tableName, "SELECT custkey, custkey, comment, orderstatus FROM orders");

        for (int i = 1; i <= 30; i++) {
            assertQuery(
                    format("SELECT * FROM %s WHERE custkey = %d AND custkey2 = %d", tableName, i, i),
                    format("SELECT custkey, custkey, comment, orderstatus FROM orders WHERE custkey = %d", i));
        }
    }

    @Test
    public void testCreateInvalidBucketedTable()
    {
        testCreateInvalidBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testCreateInvalidBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_invalid_bucketed_table";

        assertThatThrownBy(() -> computeActual("" +
                "CREATE TABLE " + tableName + " (" +
                "  a BIGINT," +
                "  b DOUBLE," +
                "  p VARCHAR" +
                ") WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'p' ], " +
                "bucketed_by = ARRAY[ 'a', 'c' ], " +
                "bucket_count = 11 " +
                ")"))
                .hasMessage("Bucketing columns [c] not present in schema");

        assertThatThrownBy(() -> computeActual("" +
                "CREATE TABLE " + tableName + " (" +
                "  a BIGINT," +
                "  b DOUBLE," +
                "  p VARCHAR" +
                ") WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'p' ], " +
                "bucketed_by = ARRAY[ 'a' ], " +
                "bucket_count = 11, " +
                "sorted_by = ARRAY[ 'c' ] " +
                ")"))
                .hasMessage("Sorting columns [c] not present in schema");

        assertThatThrownBy(() -> computeActual("" +
                "CREATE TABLE " + tableName + " (" +
                "  a BIGINT," +
                "  p VARCHAR" +
                ") WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'p' ], " +
                "bucketed_by = ARRAY[ 'p' ], " +
                "bucket_count = 11 " +
                ")"))
                .hasMessage("Bucketing columns [p] are also used as partitioning columns");

        assertThatThrownBy(() -> computeActual("" +
                "CREATE TABLE " + tableName + " (" +
                "  a BIGINT," +
                "  p VARCHAR" +
                ") WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'p' ], " +
                "bucketed_by = ARRAY[ 'a' ], " +
                "bucket_count = 11, " +
                "sorted_by = ARRAY[ 'p' ] " +
                ")"))
                .hasMessage("Sorting columns [p] are also used as partitioning columns");

        assertThatThrownBy(() -> computeActual("" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey3' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders"))
                .hasMessage("Bucketing columns [custkey3] not present in schema");

        assertThatThrownBy(() -> computeActual("" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey' ], " +
                "bucket_count = 11, " +
                "sorted_by = ARRAY[ 'custkey3' ] " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders"))
                .hasMessage("Sorting columns [custkey3] not present in schema");

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedUnionAll()
    {
        assertUpdate("CREATE TABLE test_create_partitioned_union_all (a varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");
        assertUpdate("INSERT INTO test_create_partitioned_union_all SELECT 'a', '2013-05-17' UNION ALL SELECT 'b', '2013-05-17'", 2);
        assertUpdate("DROP TABLE test_create_partitioned_union_all");
    }

    @Test
    public void testInsertPartitionedBucketedTableFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats(this::testInsertPartitionedBucketedTableFewRows);
    }

    private void testInsertPartitionedBucketedTableFewRows(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table_few_rows";

        assertUpdate(session, "" +
                "CREATE TABLE " + tableName + " (" +
                "  bucket_key varchar," +
                "  col varchar," +
                "  partition_key varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11)");

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                "INSERT INTO " + tableName + " " +
                        "VALUES " +
                        "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                        "  ('aa', 'bb', 'cc'), " +
                        "  ('aaa', 'bbb', 'ccc')",
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE test_insert_partitioned_bucketed_table_few_rows");
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    private void verifyPartitionedBucketedTableAsFewRows(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("partition_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        MaterializedResult actual = computeActual("SELECT * FROM " + tableName);
        MaterializedResult expected = resultBuilder(getSession(), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()))
                .row("a", "b", "c")
                .row("aa", "bb", "cc")
                .row("aaa", "bbb", "ccc")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testCastNullToColumnTypes()
    {
        String tableName = "test_cast_null_to_column_types";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  col1 bigint," +
                "  col2 map(bigint, bigint)," +
                "  partition_key varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'partition_key' ] " +
                ")");

        assertUpdate(format("INSERT INTO %s (col1) VALUES (1), (2), (3)", tableName), 3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyNonBucketedPartition()
    {
        String tableName = "test_insert_empty_partitioned_unbucketed_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'part' ] " +
                ")");
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 0");

        assertAccessDenied(
                format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, tableName, "empty"),
                format("Cannot insert into table hive.tpch.%s", tableName),
                privilege(tableName, INSERT_TABLE));

        // create an empty partition
        assertUpdate(format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, tableName, "empty"));
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterRegisterPartition()
    {
        String tableName = "test_register_partition_for_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  partitioned_by = ARRAY['part'] " +
                ")");

        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 0");

        assertUpdate(format("INSERT INTO %s (dummy_col, part) VALUES (1, 'first'), (2, 'second'), (3, 'third')", tableName), 3);
        List<MaterializedRow> paths = getQueryRunner().execute(getSession(), "SELECT \"$path\" FROM " + tableName + " ORDER BY \"$path\" ASC").toTestTypes().getMaterializedRows();
        assertEquals(paths.size(), 3);

        String firstPartition = new Path((String) paths.get(0).getField(0)).getParent().toString();

        assertAccessDenied(
                format("CALL system.unregister_partition('%s', '%s', ARRAY['part'], ARRAY['first'])", TPCH_SCHEMA, tableName),
                format("Cannot delete from table hive.tpch.%s", tableName),
                privilege(tableName, DELETE_TABLE));

        assertQueryFails(format("CALL system.unregister_partition('%s', '%s', ARRAY['part'], ARRAY['empty'])", TPCH_SCHEMA, tableName), "Partition 'part=empty' does not exist");
        assertUpdate(format("CALL system.unregister_partition('%s', '%s', ARRAY['part'], ARRAY['first'])", TPCH_SCHEMA, tableName));

        assertQuery(getSession(), format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 2");
        assertQuery(getSession(), "SELECT count(*) FROM " + tableName, "SELECT 2");

        assertAccessDenied(
                format("CALL system.register_partition('%s', '%s', ARRAY['part'], ARRAY['first'])", TPCH_SCHEMA, tableName),
                format("Cannot insert into table hive.tpch.%s", tableName),
                privilege(tableName, INSERT_TABLE));

        assertUpdate(format("CALL system.register_partition('%s', '%s', ARRAY['part'], ARRAY['first'], '%s')", TPCH_SCHEMA, tableName, firstPartition));

        assertQuery(getSession(), format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 3");
        assertQuery(getSession(), "SELECT count(*) FROM " + tableName, "SELECT 3");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyBucketedPartition()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testCreateEmptyBucketedPartition(storageFormat.getFormat());
        }
    }

    private void testCreateEmptyBucketedPartition(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_empty_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String sql = format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", TPCH_SCHEMA, tableName, orderStatusList.get(i));
            assertUpdate(sql);
            assertQuery(
                    format("SELECT count(*) FROM \"%s$partitions\"", tableName),
                    "SELECT " + (i + 1));

            assertQueryFails(sql, "Partition already exists.*");
        }

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreateEmptyPartitionOnNonExistingTable()
    {
        assertQueryFails(
                format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, "non_existing_table", "empty"),
                format("Table '%s.%s' does not exist", TPCH_SCHEMA, "non_existing_table"));
    }

    @Test
    public void testInsertPartitionedBucketedTable()
    {
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s'",
                            orderStatus),
                    format("SELECT count(*) FROM orders WHERE orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void createPartitionedBucketedTable(String tableName, HiveStorageFormat storageFormat)
    {
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");
    }

    @Test
    public void testInsertPartitionedBucketedTableWithUnionAll()
    {
        testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table_with_union_all";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' AND length(comment) %% 2 = 0 " +
                                    "UNION ALL " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' AND length(comment) %% 2 = 1",
                            orderStatus, orderStatus),
                    format("SELECT count(*) FROM orders WHERE orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertTwiceToSamePartitionedBucket()
    {
        String tableName = "test_insert_twice_to_same_partitioned_bucket";
        createPartitionedBucketedTable(tableName, HiveStorageFormat.RCBINARY);

        String insert = "INSERT INTO " + tableName +
                " VALUES (1, 1, 'first_comment', 'F'), (2, 2, 'second_comment', 'G')";
        assertUpdate(insert, 2);
        assertUpdate(insert, 2);

        assertQuery(
                "SELECT custkey, custkey2, comment, orderstatus FROM " + tableName + " ORDER BY custkey",
                "VALUES (1, 1, 'first_comment', 'F'), (1, 1, 'first_comment', 'F'), (2, 2, 'second_comment', 'G'), (2, 2, 'second_comment', 'G')");
        assertQuery(
                "SELECT custkey, custkey2, comment, orderstatus FROM " + tableName + " WHERE custkey = 1 and custkey2 = 1",
                "VALUES (1, 1, 'first_comment', 'F'), (1, 1, 'first_comment', 'F')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testInsert()
    {
        testWithAllStorageFormats(this::testInsert);
    }

    private void testInsert(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_format_table " +
                "(" +
                "  _string VARCHAR," +
                "  _varchar VARCHAR(65535)," +
                "  _char CHAR(10)," +
                "  _bigint BIGINT," +
                "  _integer INTEGER," +
                "  _smallint SMALLINT," +
                "  _tinyint TINYINT," +
                "  _real REAL," +
                "  _double DOUBLE," +
                "  _boolean BOOLEAN," +
                "  _decimal_short DECIMAL(3,2)," +
                "  _decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (format = '" + storageFormat + "') ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        @Language("SQL") String select = "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", 1 _bigint" +
                ", CAST(42 AS INTEGER) _integer" +
                ", CAST(43 AS SMALLINT) _smallint" +
                ", CAST(44 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (43 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (44 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_insert_format_table " + select, 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table", select);

        assertUpdate(session, "INSERT INTO test_insert_format_table (_tinyint, _smallint, _integer, _bigint, _real, _double) SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4, cast(14.3E0 as REAL), 14.3E0", 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _bigint = 4", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _real = CAST(14.3 as REAL)", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_double, _bigint) SELECT 2.72E0, 3", 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _double = CAST(2.72E0 as DOUBLE)", "SELECT null, null, null, 3, null, null, null, null, 2.72, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_decimal_short, _decimal_long) SELECT DECIMAL '2.72', DECIMAL '98765432101234567890.0123456789'", 1);

        assertQuery(session, "SELECT * FROM test_insert_format_table WHERE _decimal_long = DECIMAL '98765432101234567890.0123456789'", "SELECT null, null, null, null, null, null, null, null, null, null, 2.72, 98765432101234567890.0123456789");

        assertUpdate(session, "DROP TABLE test_insert_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_format_table"));
    }

    @Test
    public void testInsertPartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTable);
    }

    private void testInsertPartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_partitioned_table " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  SHIP_PRIORITY INTEGER," +
                "  ORDER_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        String partitionsTable = "\"test_insert_partitioned_table$partitions\"";

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT shippriority, orderstatus FROM orders LIMIT 0");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        assertUpdate(
                session,
                "" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) FROM orders");

        // verify the partitions
        List<?> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * FROM test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE no_such_column = 1", "line \\S*: Column 'no_such_column' cannot be resolved");
        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE orderkey = 1", "line \\S*: Column 'orderkey' cannot be resolved");

        assertUpdate(session, "DROP TABLE test_insert_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_partitioned_table"));
    }

    @Test
    public void testInsertPartitionedTableExistingPartition()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTableExistingPartition);
    }

    private void testInsertPartitionedTableExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) FROM orders WHERE orderkey %% 3 = %d", i));
        }

        // verify the partitions
        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * FROM " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertPartitionedTableOverwriteExistingPartition()
    {
        testInsertPartitionedTableOverwriteExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "OVERWRITE")
                        .build(),
                HiveStorageFormat.ORC);
    }

    private void testInsertPartitionedTableOverwriteExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_overwrite_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) FROM orders WHERE orderkey %% 3 = %d", i));

            // verify the partitions
            List<?> partitions = getPartitions(tableName);
            assertEquals(partitions.size(), 3);

            assertQuery(
                    session,
                    "SELECT * FROM " + tableName,
                    format("SELECT orderkey, comment, orderstatus FROM orders WHERE orderkey %% 3 = %d", i));
        }
        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testNullPartitionValues()
    {
        assertUpdate("" +
                "CREATE TABLE test_null_partition (test VARCHAR, part VARCHAR)\n" +
                "WITH (partitioned_by = ARRAY['part'])");

        assertUpdate("INSERT INTO test_null_partition VALUES ('hello', 'test'), ('world', null)", 2);

        assertQuery(
                "SELECT * FROM test_null_partition",
                "VALUES ('hello', 'test'), ('world', null)");

        assertQuery(
                "SELECT * FROM \"test_null_partition$partitions\"",
                "VALUES 'test', null");

        assertUpdate("DROP TABLE test_null_partition");
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        throw new SkipException("Covered by testInsertUnicode");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        testWithAllStorageFormats(this::testInsertUnicode);
    }

    private void testInsertUnicode(Session session, HiveStorageFormat storageFormat)
    {
        assertUpdate(session, "DROP TABLE IF EXISTS test_insert_unicode");
        assertUpdate(session, "CREATE TABLE test_insert_unicode(test varchar) WITH (format = '" + storageFormat + "')");

        // Test with U+10FFF to cover testInsertHighestUnicodeCharacter
        assertUpdate("INSERT INTO test_insert_unicode(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
        assertThat(computeActual("SELECT test FROM test_insert_unicode").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        assertUpdate(session, "DELETE FROM test_insert_unicode");

        assertUpdate(session, "INSERT INTO test_insert_unicode(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
        assertThat(computeActual(session, "SELECT test FROM test_insert_unicode").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        assertUpdate(session, "DELETE FROM test_insert_unicode");

        assertUpdate(session, "INSERT INTO test_insert_unicode(test) VALUES 'aa', 'bé'", 2);
        assertQuery(session, "SELECT test FROM test_insert_unicode", "VALUES 'aa', 'bé'");
        assertQuery(session, "SELECT test FROM test_insert_unicode WHERE test = 'aa'", "VALUES 'aa'");
        assertQuery(session, "SELECT test FROM test_insert_unicode WHERE test > 'ba'", "VALUES 'bé'");
        assertQuery(session, "SELECT test FROM test_insert_unicode WHERE test < 'ba'", "VALUES 'aa'");
        assertQueryReturnsEmptyResult(session, "SELECT test FROM test_insert_unicode WHERE test = 'ba'");
        assertUpdate(session, "DELETE FROM test_insert_unicode");

        assertUpdate(session, "INSERT INTO test_insert_unicode(test) VALUES 'a', 'é'", 2);
        assertQuery(session, "SELECT test FROM test_insert_unicode", "VALUES 'a', 'é'");
        assertQuery(session, "SELECT test FROM test_insert_unicode WHERE test = 'a'", "VALUES 'a'");
        assertQuery(session, "SELECT test FROM test_insert_unicode WHERE test > 'b'", "VALUES 'é'");
        assertQuery(session, "SELECT test FROM test_insert_unicode WHERE test < 'b'", "VALUES 'a'");
        assertQueryReturnsEmptyResult(session, "SELECT test FROM test_insert_unicode WHERE test = 'b'");

        assertUpdate(session, "DROP TABLE test_insert_unicode");
    }

    @Test
    public void testPartitionPerScanLimit()
    {
        String tableName = "test_partition_per_scan_limit";
        String partitionsTable = "\"" + tableName + "$partitions\"";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part BIGINT" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part' ]" +
                ") ";

        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("part"));

        // insert 1200 partitions
        for (int i = 0; i < 12; i++) {
            int partStart = i * 100;
            int partEnd = (i + 1) * 100 - 1;

            @Language("SQL") String insertPartitions = "" +
                    "INSERT INTO " + tableName + " " +
                    "SELECT 'bar' foo, part " +
                    "FROM UNNEST(SEQUENCE(" + partStart + ", " + partEnd + ")) AS TMP(part)";

            assertUpdate(insertPartitions, 100);
        }

        // we are not constrained by hive.max-partitions-per-scan when listing partitions
        assertQuery(
                "SELECT * FROM " + partitionsTable + " WHERE part > 490 AND part <= 500",
                "VALUES 491, 492, 493, 494, 495, 496, 497, 498, 499, 500");

        assertQuery(
                "SELECT * FROM " + partitionsTable + " WHERE part < 0",
                "SELECT null WHERE false");

        assertQuery(
                "SELECT * FROM " + partitionsTable,
                "VALUES " + LongStream.range(0, 1200)
                        .mapToObj(String::valueOf)
                        .collect(joining(",")));

        // verify can query 1000 partitions
        assertQuery(
                "SELECT count(foo) FROM " + tableName + " WHERE part < 1000",
                "SELECT 1000");

        // verify the rest 200 partitions are successfully inserted
        assertQuery(
                "SELECT count(foo) FROM " + tableName + " WHERE part >= 1000 AND part < 1200",
                "SELECT 200");

        // verify cannot query more than 1000 partitions
        assertQueryFails(
                "SELECT * FROM " + tableName + " WHERE part < 1001",
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        // verify cannot query all partitions
        assertQueryFails(
                "SELECT * FROM " + tableName,
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionPerScanLimitWithMultiplePartitionColumns()
    {
        String tableName = "test_multi_partition_per_scan_limit";
        String partitionsTable = "\"" + tableName + "$partitions\"";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (  foo varchar,   part1 bigint,   part2 bigint) " +
                "WITH (partitioned_by = ARRAY['part1', 'part2'])");

        // insert 1200 partitions with part1 being NULL
        for (int batchNumber = 0; batchNumber < 12; batchNumber++) {
            // insert with a loop to avoid max open writers limit
            assertUpdate(
                    format(
                            "INSERT INTO %s SELECT 'bar' foo, NULL part1, n part2 FROM UNNEST(sequence(%s, %s)) a(n)",
                            tableName,
                            batchNumber * 100 + 1,
                            (batchNumber + 1) * 100),
                    100);
        }
        // insert 10 partitions with part1=part2
        assertUpdate("INSERT INTO " + tableName + " SELECT 'bar' foo, n part1, n part2 FROM UNNEST(sequence(1, 10)) a(n)", 10);

        // verify can query 1000 partitions
        assertThat(query("SELECT count(*) FROM " + tableName + " WHERE part1 IS NULL AND part2 < 1001"))
                .matches("VALUES BIGINT '1000'");

        // verify cannot query more than 1000 partitions
        assertThatThrownBy(() -> query("SELECT count(*) FROM " + tableName + " WHERE part1 IS NULL AND part2 <= 1001"))
                .hasMessage("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName);
        assertThatThrownBy(() -> query("SELECT count(*) FROM " + tableName))
                .hasMessage("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName);

        // verify we can query with a predicate that is not representable as a TupleDomain
        assertThat(query("SELECT * FROM " + tableName + " WHERE part1 % 400 = 3")) // may be translated to Domain.all
                .matches("VALUES (VARCHAR 'bar', BIGINT '3', BIGINT '3')");
        assertThat(query("SELECT * FROM " + tableName + " WHERE part1 % 400 = 3 AND part1 IS NOT NULL"))  // may be translated to Domain.all except nulls
                .matches("VALUES (VARCHAR 'bar', BIGINT '3', BIGINT '3')");

        // we are not constrained by hive.max-partitions-per-scan (=1000) when listing partitions
        assertThat(query("SELECT * FROM " + partitionsTable))
                .matches("" +
                        "SELECT CAST(NULL AS bigint), CAST(n AS bigint) FROM UNNEST(sequence(1, 1200)) a(n) " +
                        "UNION ALL VALUES (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10, 10)");
        assertThat(query("SELECT * FROM " + partitionsTable + " WHERE part1 IS NOT NULL"))
                .matches("VALUES (BIGINT '1', BIGINT '1'), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10, 10)");
        assertThat(query("SELECT * FROM " + partitionsTable + " WHERE part1 < 4"))
                .matches("VALUES (BIGINT '1', BIGINT '1'), (2,2), (3,3)");
        assertThat(query("SELECT * FROM " + partitionsTable + " WHERE part2 < 4"))
                .matches("VALUES (BIGINT '1', BIGINT '1'), (2,2), (3,3), (NULL,1), (NULL,2), (NULL,3)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowColumnsFromPartitions()
    {
        String tableName = "test_show_columns_from_partitions";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQuery(
                getSession(),
                "SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                "VALUES ('part1', 'bigint', '', ''), ('part2', 'varchar', '', '')");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"$partitions\"",
                ".*Table '.*\\.tpch\\.\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"orders$partitions\"",
                ".*Table '.*\\.tpch\\.orders\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"blah$partitions\"",
                ".*Table '.*\\.tpch\\.blah\\$partitions' does not exist");
    }

    @Test
    public void testPartitionsTableInvalidAccess()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitions_invalid " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"test_partitions_invalid$partitions$partitions\"",
                ".*Table '.*\\.tpch\\.test_partitions_invalid\\$partitions\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"non_existent$partitions\"",
                ".*Table '.*\\.tpch\\.non_existent\\$partitions' does not exist");
    }

    @Test
    public void testInsertUnpartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertUnpartitionedTable);
    }

    private void testInsertUnpartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_unpartitioned_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "'" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) FROM orders WHERE orderkey %% 3 = %d", i));
        }

        assertQuery(
                session,
                "SELECT * FROM " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned AS SELECT orderstatus FROM tpch.tiny.orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete_unpartitioned");

        MaterializedResult result = computeActual("SELECT * FROM test_delete_unpartitioned");
        assertEquals(result.getRowCount(), 0);

        assertUpdate("DROP TABLE test_delete_unpartitioned");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_delete_unpartitioned"));
    }

    @Test
    public void testMetadataDelete()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_metadata_delete " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  LINE_NUMBER INTEGER," +
                "  LINE_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                PARTITIONED_BY_PROPERTY + " = ARRAY[ 'LINE_NUMBER', 'LINE_STATUS' ]" +
                ") ";

        assertUpdate(createTable);

        assertUpdate("" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, linenumber, linestatus " +
                        "FROM tpch.tiny.lineitem",
                "SELECT count(*) FROM lineitem");

        // Delete returns number of rows deleted, or null if obtaining the number is hard or impossible.
        // Currently, Hive implementation always returns null.
        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='F' AND LINE_NUMBER=CAST(3 AS INTEGER)");

        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'F' or linenumber<>3");

        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='O'");

        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' AND linenumber<>3");

        assertThatThrownBy(() -> getQueryRunner().execute("DELETE FROM test_metadata_delete WHERE ORDER_KEY=1"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Deletes must match whole partitions for non-transactional tables");

        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' AND linenumber<>3");

        assertUpdate("DROP TABLE test_metadata_delete");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_metadata_delete"));
    }

    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = getDistributedQueryRunner().getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, schema, tableName));
                    assertTrue(tableHandle.isPresent());
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }

    private Object getHiveTableProperty(String tableName, Function<HiveTableHandle, Object> propertyGetter)
    {
        Session session = getSession();
        Metadata metadata = getDistributedQueryRunner().getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    QualifiedObjectName name = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    TableHandle table = metadata.getTableHandle(transactionSession, name)
                            .orElseThrow(() -> new AssertionError("table not found: " + name));
                    table = metadata.applyFilter(transactionSession, table, Constraint.alwaysTrue())
                            .orElseThrow(() -> new AssertionError("applyFilter did not return a result"))
                            .getHandle();
                    return propertyGetter.apply((HiveTableHandle) table.getConnectorHandle());
                });
    }

    private List<?> getPartitions(String tableName)
    {
        return (List<?>) getHiveTableProperty(tableName, handle -> handle.getPartitions().get());
    }

    private int getBucketCount(String tableName)
    {
        return (int) getHiveTableProperty(tableName, table -> table.getBucketHandle().get().getTableBucketCount());
    }

    @Test
    public void testShowColumnsPartitionKey()
    {
        assertUpdate("" +
                "CREATE TABLE test_show_columns_partition_key\n" +
                "(grape bigint, orange bigint, pear varchar(65535), mango integer, lychee smallint, kiwi tinyint, apple varchar, pineapple varchar(65535))\n" +
                "WITH (partitioned_by = ARRAY['apple', 'pineapple'])");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM test_show_columns_partition_key");
        Type unboundedVarchar = canonicalizeType(VARCHAR);
        MaterializedResult expected = resultBuilder(getSession(), unboundedVarchar, unboundedVarchar, unboundedVarchar, unboundedVarchar)
                .row("grape", canonicalizeType(BIGINT).toString(), "", "")
                .row("orange", canonicalizeType(BIGINT).toString(), "", "")
                .row("pear", canonicalizeType(createVarcharType(65535)).toString(), "", "")
                .row("mango", canonicalizeType(INTEGER).toString(), "", "")
                .row("lychee", canonicalizeType(SMALLINT).toString(), "", "")
                .row("kiwi", canonicalizeType(TINYINT).toString(), "", "")
                .row("apple", canonicalizeType(VARCHAR).toString(), "partition key", "")
                .row("pineapple", canonicalizeType(createVarcharType(65535)).toString(), "partition key", "")
                .build();
        assertEquals(actual, expected);
    }

    // TODO: These should be moved to another class, when more connectors support arrays
    @Test
    public void testArrays()
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");

        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[ARRAY[INTEGER'1', INTEGER'2'], NULL, ARRAY[INTEGER'3', INTEGER'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array7", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[ARRAY[SMALLINT'1', SMALLINT'2'], NULL, ARRAY[SMALLINT'3', SMALLINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array8", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array9 AS SELECT ARRAY[ARRAY[TINYINT'1', TINYINT'2'], NULL, ARRAY[TINYINT'3', TINYINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array9", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array10 AS SELECT ARRAY[ARRAY[DECIMAL '3.14']] AS col1, ARRAY[ARRAY[DECIMAL '12345678901234567890.0123456789']] AS col2", 1);
        assertQuery("SELECT col1[1][1] FROM tmp_array10", "SELECT 3.14");
        assertQuery("SELECT col2[1][1] FROM tmp_array10", "SELECT 12345678901234567890.0123456789");

        assertUpdate("CREATE TABLE tmp_array13 AS SELECT ARRAY[ARRAY[REAL'1.234', REAL'2.345'], NULL, ARRAY[REAL'3.456', REAL'4.567']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array13", "SELECT 2.345");
    }

    @Test(dataProvider = "timestampPrecision")
    public void testTemporalArrays(HiveTimestampPrecision timestampPrecision)
    {
        Session session = withTimestampPrecision(getSession(), timestampPrecision);
        assertUpdate("DROP TABLE IF EXISTS tmp_array11");
        assertUpdate("CREATE TABLE tmp_array11 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array11");
        assertUpdate("DROP TABLE IF EXISTS tmp_array12");
        assertUpdate("CREATE TABLE tmp_array12 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult(session, "SELECT col[1] FROM tmp_array12");
    }

    @Test(dataProvider = "timestampPrecision")
    public void testMaps(HiveTimestampPrecision timestampPrecision)
    {
        Session session = withTimestampPrecision(getSession(), timestampPrecision);
        assertUpdate("DROP TABLE IF EXISTS tmp_map1");
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("DROP TABLE IF EXISTS tmp_map2");
        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[INTEGER'1'], ARRAY[INTEGER'2']) AS col", 1);
        assertQuery("SELECT col[INTEGER'1'] FROM tmp_map2", "SELECT 2");

        assertUpdate("DROP TABLE IF EXISTS tmp_map3");
        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY[SMALLINT'1'], ARRAY[SMALLINT'2']) AS col", 1);
        assertQuery("SELECT col[SMALLINT'1'] FROM tmp_map3", "SELECT 2");

        assertUpdate("DROP TABLE IF EXISTS tmp_map4");
        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TINYINT'1'], ARRAY[TINYINT'2']) AS col", 1);
        assertQuery("SELECT col[TINYINT'1'] FROM tmp_map4", "SELECT 2");

        assertUpdate("DROP TABLE IF EXISTS tmp_map5");
        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map5", "SELECT 2.5");

        assertUpdate("DROP TABLE IF EXISTS tmp_map6");
        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map6", "SELECT 'kittens'");

        assertUpdate("DROP TABLE IF EXISTS tmp_map7");
        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", 1);
        assertQuery("SELECT col[TRUE] FROM tmp_map7", "SELECT FALSE");

        assertUpdate("DROP TABLE IF EXISTS tmp_map8");
        assertUpdate("CREATE TABLE tmp_map8 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map8");

        assertUpdate("DROP TABLE IF EXISTS tmp_map9");
        assertUpdate("CREATE TABLE tmp_map9 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult(session, "SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map9");

        assertUpdate("DROP TABLE IF EXISTS tmp_map10");
        assertUpdate("CREATE TABLE tmp_map10 AS SELECT MAP(ARRAY[DECIMAL '3.14', DECIMAL '12345678901234567890.0123456789'], " +
                "ARRAY[DECIMAL '12345678901234567890.0123456789', DECIMAL '3.0123456789']) AS col", 1);
        assertQuery("SELECT col[DECIMAL '3.14'], col[DECIMAL '12345678901234567890.0123456789'] FROM tmp_map10", "SELECT 12345678901234567890.0123456789, 3.0123456789");

        assertUpdate("DROP TABLE IF EXISTS tmp_map11");
        assertUpdate("CREATE TABLE tmp_map11 AS SELECT MAP(ARRAY[REAL'1.234'], ARRAY[REAL'2.345']) AS col", 1);
        assertQuery("SELECT col[REAL'1.234'] FROM tmp_map11", "SELECT 2.345");

        assertUpdate("DROP TABLE IF EXISTS tmp_map12");
        assertUpdate("CREATE TABLE tmp_map12 AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map12", "SELECT 2");
    }

    @Test
    public void testRowsWithAllFormats()
    {
        testWithAllStorageFormats(this::testRows);
    }

    private void testRows(Session session, HiveStorageFormat format)
    {
        String tableName = "test_dereferences";
        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName +
                " WITH (" +
                "format = '" + format + "'" +
                ") " +
                "AS SELECT " +
                "CAST(row(CAST(1 as BIGINT), CAST(NULL as BIGINT)) AS row(col0 bigint, col1 bigint)) AS a, " +
                "CAST(row(row(VARCHAR 'abc', CAST(5 as BIGINT)), CAST(3.0 AS DOUBLE)) AS row(field0 row(col0 varchar, col1 bigint), field1 double)) AS b";

        assertUpdate(session, createTable, 1);

        assertQuery(session,
                "SELECT a.col0, a.col1, b.field0.col0, b.field0.col1, b.field1 FROM " + tableName,
                "SELECT 1, cast(null as bigint), CAST('abc' AS varchar), CAST(5 as BIGINT), CAST(3.0 AS DOUBLE)");

        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testRowsWithNulls()
    {
        testRowsWithNulls(getSession(), HiveStorageFormat.ORC);
        testRowsWithNulls(getSession(), HiveStorageFormat.PARQUET);
    }

    private void testRowsWithNulls(Session session, HiveStorageFormat format)
    {
        String tableName = "test_dereferences_with_nulls";
        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + "\n" +
                "(col0 BIGINT, col1 row(f0 BIGINT, f1 BIGINT), col2 row(f0 BIGINT, f1 ROW(f0 BIGINT, f1 BIGINT)))\n" +
                "WITH (format = '" + format + "')";

        assertUpdate(session, createTable);

        @Language("SQL") String insertTable = "" +
                "INSERT INTO " + tableName + " VALUES \n" +
                "row(1,     row(2, 3),      row(4, row(5, 6))),\n" +
                "row(7,     row(8, 9),      row(10, row(11, NULL))),\n" +
                "row(NULL,  NULL,           row(12, NULL)),\n" +
                "row(13,    row(NULL, 14),  NULL),\n" +
                "row(15,    row(16, NULL),  row(NULL, row(17, 18)))";

        assertUpdate(session, insertTable, 5);

        assertQuery(
                session,
                format("SELECT col0, col1.f0, col2.f1.f1 FROM %s", tableName),
                "SELECT * FROM \n" +
                        "    (SELECT 1, 2, 6) UNION\n" +
                        "    (SELECT 7, 8, NULL) UNION\n" +
                        "    (SELECT NULL, NULL, NULL) UNION\n" +
                        "    (SELECT 13, NULL, NULL) UNION\n" +
                        "    (SELECT 15, 16, 18)");

        assertQuery(session, format("SELECT col0 FROM %s WHERE col2.f1.f1 IS NOT NULL", tableName), "SELECT * FROM UNNEST(array[1, 15])");

        assertQuery(session, format("SELECT col0, col1.f0, col1.f1 FROM %s WHERE col2.f1.f1 = 18", tableName), "SELECT 15, 16, NULL");

        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testComplex()
    {
        assertUpdate("CREATE TABLE tmp_complex1 AS SELECT " +
                        "ARRAY [MAP(ARRAY['a', 'b'], ARRAY[2.0E0, 4.0E0]), MAP(ARRAY['c', 'd'], ARRAY[12.0E0, 14.0E0])] AS a",
                1);

        assertQuery(
                "SELECT a[1]['a'], a[2]['d'] FROM tmp_complex1",
                "SELECT 2.0, 14.0");
    }

    @Test
    public void testBucketedCatalog()
    {
        String bucketedCatalog = bucketedSession.getCatalog().get();
        String bucketedSchema = bucketedSession.getSchema().get();

        TableMetadata ordersTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "orders");
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        TableMetadata customerTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "customer");
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);
    }

    @Test
    public void testBucketedExecution()
    {
        assertQuery(bucketedSession, "SELECT count(*) a FROM orders t1 JOIN orders t2 on t1.custkey=t2.custkey");
        assertQuery(bucketedSession, "SELECT count(*) a FROM orders t1 JOIN customer t2 on t1.custkey=t2.custkey", "SELECT count(*) FROM orders");
        assertQuery(bucketedSession, "SELECT count(distinct custkey) FROM orders");

        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "1").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "4").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testScaleWriters()
    {
        testWithAllStorageFormats(this::testSingleWriter);
        testWithAllStorageFormats(this::testMultipleWriters);
    }

    protected void testSingleWriter(Session session, HiveStorageFormat storageFormat)
    {
        try {
            // small table that will only have one writer
            @Language("SQL") String createTableSql = format("" +
                            "CREATE TABLE scale_writers_small WITH (format = '%s') AS " +
                            "SELECT * FROM tpch.tiny.orders",
                    storageFormat);
            assertUpdate(
                    Session.builder(session)
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "32MB")
                            .build(),
                    createTableSql,
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());

            assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM scale_writers_small").getOnlyValue(), 1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_small");
        }
    }

    private void testMultipleWriters(Session session, HiveStorageFormat storageFormat)
    {
        try {
            // large table that will scale writers to multiple machines
            @Language("SQL") String createTableSql = format("" +
                            "CREATE TABLE scale_writers_large WITH (format = '%s') AS " +
                            "SELECT * FROM tpch.sf1.orders",
                    storageFormat);
            assertUpdate(
                    Session.builder(session)
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "1MB")
                            .setCatalogSessionProperty(catalog, "parquet_writer_block_size", "4MB")
                            .build(),
                    createTableSql,
                    (long) computeActual("SELECT count(*) FROM tpch.sf1.orders").getOnlyValue());

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM scale_writers_large");
            long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
            assertThat(files).isBetween(2L, workers);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_large");
        }
    }

    @Test
    public void testTableCommentsTable()
    {
        assertUpdate("CREATE TABLE test_comment (c1 bigint) COMMENT 'foo'");
        String selectTableComment = format("" +
                        "SELECT comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = 'test_comment'",
                getSession().getCatalog().get(),
                getSession().getSchema().get());
        assertQuery(selectTableComment, "SELECT 'foo'");

        assertUpdate("DROP TABLE IF EXISTS test_comment");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE hive.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")");

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_show_create_table");

        assertUpdate(createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   \"c 2\" varchar,\n" +
                        "   \"c'3\" array(bigint),\n" +
                        "   c4 map(bigint, varchar) COMMENT 'comment test4',\n" +
                        "   c5 double COMMENT ''\n)\n" +
                        "COMMENT 'test'\n" +
                        "WITH (\n" +
                        "   bucket_count = 5,\n" +
                        "   bucketed_by = ARRAY['c1','c 2'],\n" +
                        "   bucketing_version = 1,\n" +
                        "   format = 'ORC',\n" +
                        "   orc_bloom_filter_columns = ARRAY['c1','c2'],\n" +
                        "   orc_bloom_filter_fpp = 7E-1,\n" +
                        "   partitioned_by = ARRAY['c5'],\n" +
                        "   sorted_by = ARRAY['c1','c 2 DESC'],\n" +
                        "   transactional = true\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "\"test_show_create_table'2\"");
        assertUpdate(createTableSql);
        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table'2\"");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 ROW(\"$a\" bigint, \"$b\" varchar)\n)\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_show_create_table_with_special_characters");
        assertUpdate(createTableSql);
        actualResult = computeActual("SHOW CREATE TABLE test_show_create_table_with_special_characters");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);
    }

    private void testCreateExternalTable(
            String tableName,
            String fileContents,
            String expectedResults,
            List<String> tableProperties)
            throws Exception
    {
        java.nio.file.Path tempDir = createTempDirectory(null);
        File dataFile = tempDir.resolve("test.txt").toFile();
        Files.asCharSink(dataFile, UTF_8).write(fileContents);

        // Table properties
        StringJoiner propertiesSql = new StringJoiner(",\n   ");
        propertiesSql.add(
                format("external_location = '%s'", new Path(tempDir.toUri().toASCIIString())));
        propertiesSql.add("format = 'TEXTFILE'");
        tableProperties.forEach(propertiesSql::add);

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   col1 varchar,\n" +
                        "   col2 varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   %s\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                propertiesSql);

        assertUpdate(createTableSql);
        MaterializedResult actual = computeActual(format("SHOW CREATE TABLE %s", tableName));
        assertEquals(actual.getOnlyValue(), createTableSql);

        assertQuery(format("SELECT col1, col2 from %s", tableName), expectedResults);
        assertUpdate(format("DROP TABLE %s", tableName));
        assertFile(dataFile); // file should still exist after drop
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testCreateExternalTable()
            throws Exception
    {
        testCreateExternalTable(
                "test_create_external",
                "hello\u0001world\nbye\u0001world",
                "VALUES ('hello', 'world'), ('bye', 'world')",
                ImmutableList.of());
    }

    @Test
    public void testCreateExternalTableWithFieldSeparator()
            throws Exception
    {
        testCreateExternalTable(
                "test_create_external_with_field_separator",
                "helloXworld\nbyeXworld",
                "VALUES ('hello', 'world'), ('bye', 'world')",
                ImmutableList.of("textfile_field_separator = 'X'"));
    }

    @Test
    public void testCreateExternalTableWithFieldSeparatorUnescaped()
            throws Exception
    {
        testCreateExternalTable(
                "test_create_external_with_field_separator_unescaped",
                "heXlloXworld\nbyeXworld", // the first line contains an unescaped separator character which leads to inconsistent reading of its content
                "VALUES ('he', 'llo'), ('bye', 'world')",
                ImmutableList.of("textfile_field_separator = 'X'"));
    }

    @Test
    public void testCreateExternalTableWithFieldSeparatorEscape()
            throws Exception
    {
        testCreateExternalTable(
                "test_create_external_text_file_with_field_separator_and_escape",
                "HelloEFFWorld\nByeEFFWorld",
                "VALUES ('HelloF', 'World'), ('ByeF', 'World')",
                ImmutableList.of(
                        "textfile_field_separator = 'F'",
                        "textfile_field_separator_escape = 'E'"));
    }

    @Test
    public void testCreateExternalTableWithNullFormat()
            throws Exception
    {
        testCreateExternalTable(
                "test_create_external_textfile_with_null_format",
                "hello\u0001NULL_VALUE\nNULL_VALUE\u0001123\n\\N\u0001456",
                "VALUES ('hello', NULL), (NULL, 123), ('\\N', 456)",
                ImmutableList.of("null_format = 'NULL_VALUE'"));
    }

    @Test
    public void testCreateExternalTableWithDataNotAllowed()
            throws IOException
    {
        java.nio.file.Path tempDir = createTempDirectory(null);

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE test_create_external_with_data_not_allowed " +
                        "WITH (external_location = '%s') AS " +
                        "SELECT * FROM tpch.tiny.nation",
                tempDir.toUri().toASCIIString());

        assertQueryFails(createTableSql, "Writes to non-managed Hive tables is disabled");
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    private void testCreateTableWithHeaderAndFooter(String format)
    {
        String name = format.toLowerCase(ENGLISH);
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s_table_skip_header (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '%s',\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                catalog, schema, name, format);

        assertUpdate(createTableSql);

        MaterializedResult actual = computeActual(format("SHOW CREATE TABLE %s_table_skip_header", format));
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate(format("DROP TABLE %s_table_skip_header", format));

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s_table_skip_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '%s',\n" +
                        "   skip_footer_line_count = 1\n" +
                        ")",
                catalog, schema, name, format);

        assertUpdate(createTableSql);

        actual = computeActual(format("SHOW CREATE TABLE %s_table_skip_footer", format));
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate(format("DROP TABLE %s_table_skip_footer", format));

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s_table_skip_header_footer (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '%s',\n" +
                        "   skip_footer_line_count = 1,\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                catalog, schema, name, format);

        assertUpdate(createTableSql);

        actual = computeActual(format("SHOW CREATE TABLE %s_table_skip_header_footer", format));
        assertEquals(actual.getOnlyValue(), createTableSql);
        assertUpdate(format("DROP TABLE %s_table_skip_header_footer", format));

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s_table_skip_header " +
                        "WITH (\n" +
                        "   format = '%s',\n" +
                        "   skip_header_line_count = 1\n" +
                        ") AS SELECT CAST(1 AS VARCHAR) AS col_name1, CAST(2 AS VARCHAR) as col_name2",
                catalog, schema, name, format);

        assertUpdate(createTableSql, 1);
        assertUpdate(format("INSERT INTO %s.%s.%s_table_skip_header VALUES('3', '4')", catalog, schema, name), 1);
        MaterializedResult materializedRows = computeActual(format("SELECT * FROM %s_table_skip_header", name));
        assertEqualsIgnoreOrder(materializedRows, resultBuilder(getSession(), VARCHAR, VARCHAR)
                .row("1", "2")
                .row("3", "4")
                .build()
                .getMaterializedRows());
        assertUpdate(format("DROP TABLE %s_table_skip_header", format));
    }

    @Test
    public void testCreateTableWithHeaderAndFooterForTextFile()
    {
        testCreateTableWithHeaderAndFooter("TEXTFILE");
    }

    @Test
    public void testCreateTableWithHeaderAndFooterForCsv()
    {
        testCreateTableWithHeaderAndFooter("CSV");
    }

    @Test
    public void testInsertTableWithHeaderAndFooterForCsv()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_header (\n" +
                        "   name VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_header_line_count = 2\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        assertThatThrownBy(() -> assertUpdate(
                format("INSERT INTO %s.%s.csv_table_skip_header VALUES ('name')",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get())))
                .hasMessageMatching("Inserting into Hive table with value of skip.header.line.count property greater than 1 is not supported");

        assertUpdate("DROP TABLE csv_table_skip_header");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_footer (\n" +
                        "   name VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_footer_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        assertThatThrownBy(() -> assertUpdate(
                format("INSERT INTO %s.%s.csv_table_skip_footer VALUES ('name')",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get())))
                .hasMessageMatching("Inserting into Hive table with skip.footer.line.count property not supported");

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.csv_table_skip_header_footer (\n" +
                        "   name VARCHAR\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'CSV',\n" +
                        "   skip_footer_line_count = 1,\n" +
                        "   skip_header_line_count = 1\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertUpdate(createTableSql);

        assertThatThrownBy(() -> assertUpdate(
                format("INSERT INTO %s.%s.csv_table_skip_header_footer VALUES ('name')",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get())))
                .hasMessageMatching("Inserting into Hive table with skip.footer.line.count property not supported");

        assertUpdate("DROP TABLE csv_table_skip_header_footer");
    }

    @Test
    public void testCreateTableWithInvalidProperties()
    {
        // ORC
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'TEXTFILE', orc_bloom_filter_columns = ARRAY['col1'])"))
                .hasMessageMatching("Cannot specify orc_bloom_filter_columns table property for storage format: TEXTFILE");

        // TEXTFILE
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_orc_skip_header (col1 bigint) WITH (format = 'ORC', skip_header_line_count = 1)"))
                .hasMessageMatching("Cannot specify skip_header_line_count table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_orc_skip_footer (col1 bigint) WITH (format = 'ORC', skip_footer_line_count = 1)"))
                .hasMessageMatching("Cannot specify skip_footer_line_count table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_orc_skip_footer (col1 bigint) WITH (format = 'ORC', null_format = 'ERROR')"))
                .hasMessageMatching("Cannot specify null_format table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_invalid_skip_header (col1 bigint) WITH (format = 'TEXTFILE', skip_header_line_count = -1)"))
                .hasMessageMatching("Invalid value for skip_header_line_count property: -1");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE test_invalid_skip_footer (col1 bigint) WITH (format = 'TEXTFILE', skip_footer_line_count = -1)"))
                .hasMessageMatching("Invalid value for skip_footer_line_count property: -1");

        // CSV
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_separator = 'S')"))
                .hasMessageMatching("Cannot specify csv_separator table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_separator = 'SS')"))
                .hasMessageMatching("csv_separator must be a single character string, but was: 'SS'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_quote = 'Q')"))
                .hasMessageMatching("Cannot specify csv_quote table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_quote = 'QQ')"))
                .hasMessageMatching("csv_quote must be a single character string, but was: 'QQ'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'ORC', csv_escape = 'E')"))
                .hasMessageMatching("Cannot specify csv_escape table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_escape = 'EE')"))
                .hasMessageMatching("csv_escape must be a single character string, but was: 'EE'");
    }

    @Test
    public void testPathHiddenColumn()
    {
        testWithAllStorageFormats(this::testPathHiddenColumn);
    }

    private void testPathHiddenColumn(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "CREATE TABLE test_path " +
                "WITH (" +
                "format = '" + storageFormat + "'," +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(session, createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_path"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_path");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(PATH_COLUMN_NAME)) {
                // $path should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_path").size(), 3);

        MaterializedResult results = computeActual(session, format("SELECT *, \"%s\" FROM test_path", PATH_COLUMN_NAME));
        Map<Integer, String> partitionPathMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            String pathName = (String) row.getField(2);
            String parentDirectory = new Path(pathName).getParent().toString();

            assertTrue(pathName.length() > 0);
            assertEquals(col0 % 3, col1);
            if (partitionPathMap.containsKey(col1)) {
                // the rows in the same partition should be in the same partition directory
                assertEquals(partitionPathMap.get(col1), parentDirectory);
            }
            else {
                partitionPathMap.put(col1, parentDirectory);
            }
        }
        assertEquals(partitionPathMap.size(), 3);

        assertUpdate(session, "DROP TABLE test_path");
        assertFalse(getQueryRunner().tableExists(session, "test_path"));
    }

    @Test
    public void testBucketHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_bucket_hidden_column " +
                "WITH (" +
                "bucketed_by = ARRAY['col0']," +
                "bucket_count = 2" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 11), (1, 12), (2, 13), " +
                "(3, 14), (4, 15), (5, 16), " +
                "(6, 17), (7, 18), (8, 19)" +
                " ) t (col0, col1) ";
        assertUpdate(createTable, 9);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_bucket_hidden_column");
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("col0"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 2);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, BUCKET_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(BUCKET_COLUMN_NAME)) {
                // $bucket_number should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getBucketCount("test_bucket_hidden_column"), 2);

        MaterializedResult results = computeActual(format("SELECT *, \"%1$s\" FROM test_bucket_hidden_column WHERE \"%1$s\" = 1",
                BUCKET_COLUMN_NAME));
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            int bucket = (int) row.getField(2);

            assertEquals(col1, col0 + 11);
            assertTrue(col1 % 2 == 0);

            // Because Hive's hash function for integer n is h(n) = n.
            assertEquals(bucket, col0 % 2);
        }
        assertEquals(results.getRowCount(), 4);

        assertUpdate("DROP TABLE test_bucket_hidden_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));
    }

    @Test
    public void testFileSizeHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_file_size " +
                "WITH (" +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_file_size"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_size");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_SIZE_COLUMN_NAME)) {
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_file_size").size(), 3);

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_file_size", FILE_SIZE_COLUMN_NAME));
        Map<Integer, Long> fileSizeMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            long fileSize = (Long) row.getField(2);

            assertTrue(fileSize > 0);
            assertEquals(col0 % 3, col1);
            if (fileSizeMap.containsKey(col1)) {
                assertEquals(fileSizeMap.get(col1).longValue(), fileSize);
            }
            else {
                fileSizeMap.put(col1, fileSize);
            }
        }
        assertEquals(fileSizeMap.size(), 3);

        assertUpdate("DROP TABLE test_file_size");
    }

    @Test(dataProvider = "timestampPrecision")
    public void testFileModifiedTimeHiddenColumn(HiveTimestampPrecision precision)
    {
        long testStartTime = Instant.now().toEpochMilli();

        @Language("SQL") String createTable = "CREATE TABLE test_file_modified_time " +
                "WITH (" +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_file_modified_time"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_modified_time");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_file_modified_time").size(), 3);

        Session sessionWithTimestampPrecision = withTimestampPrecision(getSession(), precision);
        MaterializedResult results = computeActual(
                sessionWithTimestampPrecision,
                format("SELECT *, \"%s\" FROM test_file_modified_time", FILE_MODIFIED_TIME_COLUMN_NAME));
        Map<Integer, Instant> fileModifiedTimeMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            Instant fileModifiedTime = ((ZonedDateTime) row.getField(2)).toInstant();

            assertThat(fileModifiedTime.toEpochMilli()).isCloseTo(testStartTime, offset(2000L));
            assertEquals(col0 % 3, col1);
            if (fileModifiedTimeMap.containsKey(col1)) {
                assertEquals(fileModifiedTimeMap.get(col1), fileModifiedTime);
            }
            else {
                fileModifiedTimeMap.put(col1, fileModifiedTime);
            }
        }
        assertEquals(fileModifiedTimeMap.size(), 3);

        assertUpdate("DROP TABLE test_file_modified_time");
    }

    @Test
    public void testPartitionHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_partition_hidden_column " +
                "WITH (" +
                "partitioned_by = ARRAY['col1', 'col2']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 11, 21), (1, 12, 22), (2, 13, 23), " +
                "(3, 14, 24), (4, 15, 25), (5, 16, 26), " +
                "(6, 17, 27), (7, 18, 28), (8, 19, 29)" +
                " ) t (col0, col1, col2) ";
        assertUpdate(createTable, 9);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_partition_hidden_column"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partition_hidden_column");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("col1", "col2"));

        List<String> columnNames = ImmutableList.of("col0", "col1", "col2", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(PARTITION_COLUMN_NAME)) {
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_partition_hidden_column").size(), 9);

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_partition_hidden_column", PARTITION_COLUMN_NAME));
        for (MaterializedRow row : results.getMaterializedRows()) {
            String actualPartition = (String) row.getField(3);
            String expectedPartition = format("col1=%s/col2=%s", row.getField(1), row.getField(2));
            assertEquals(actualPartition, expectedPartition);
        }
        assertEquals(results.getRowCount(), 9);

        assertUpdate("DROP TABLE test_partition_hidden_column");
    }

    @Test
    public void testDeleteAndInsert()
    {
        Session session = getSession();

        // Partition 1 is untouched
        // Partition 2 is altered (dropped and then added back)
        // Partition 3 is added
        // Partition 4 is dropped

        assertUpdate(
                session,
                "CREATE TABLE tmp_delete_insert WITH (partitioned_by=array ['z']) AS " +
                        "SELECT * FROM (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2), (401, 4), (402, 4), (403, 4)) t(a, z)",
                6);

        List<MaterializedRow> expectedBefore = resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(401L, 4L)
                .row(402L, 4L)
                .row(403L, 4L)
                .build()
                .getMaterializedRows();
        List<MaterializedRow> expectedAfter = resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(203L, 2L)
                .row(204L, 2L)
                .row(205L, 2L)
                .row(301L, 2L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        try {
            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                    .execute(session, transactionSession -> {
                        assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                        assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                        MaterializedResult actualFromAnotherTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromAnotherTransaction, expectedBefore);
                        MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromCurrentTransaction, expectedAfter);
                        rollback();
                    });
        }
        catch (RollbackException e) {
            // ignore
        }

        MaterializedResult actualAfterRollback = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterRollback, expectedBefore);

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                    assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                    MaterializedResult actualOutOfTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualOutOfTransaction, expectedBefore);
                    MaterializedResult actualInTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualInTransaction, expectedAfter);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expectedAfter);
    }

    @Test
    public void testCreateAndInsert()
    {
        Session session = getSession();

        List<MaterializedRow> expected = resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(301L, 3L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(
                            transactionSession,
                            "CREATE TABLE tmp_create_insert WITH (partitioned_by=array ['z']) AS " +
                                    "SELECT * FROM (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2)) t(a, z)",
                            3);
                    assertUpdate(transactionSession, "INSERT INTO tmp_create_insert VALUES (301, 3), (302, 3)", 2);
                    MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_create_insert");
                    assertEqualsIgnoreOrder(actualFromCurrentTransaction, expected);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_create_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expected);
    }

    @Test
    public void testRenameView()
    {
        assertUpdate("CREATE VIEW rename_view_original AS SELECT COUNT(*) as count FROM orders");

        assertQuery("SELECT * FROM rename_view_original", "SELECT COUNT(*) FROM orders");

        assertUpdate("CREATE SCHEMA view_rename");

        assertUpdate("ALTER VIEW rename_view_original RENAME TO view_rename.rename_view_new");

        assertQuery("SELECT * FROM view_rename.rename_view_new", "SELECT COUNT(*) FROM orders");

        assertQueryFails("SELECT * FROM rename_view_original", ".*rename_view_original' does not exist");

        assertUpdate("DROP VIEW view_rename.rename_view_new");
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        super.testRenameColumn();

        // Additional tests for hive partition columns invariants
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_rename_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN orderkey TO new_orderkey");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN \"$path\" TO test", ".* Cannot rename hidden column");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN orderstatus TO new_orderstatus", "Renaming partition columns is not supported.*");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertUpdate("DROP TABLE test_rename_column");
    }

    @Test
    @Override
    public void testDropColumn()
    {
        super.testDropColumn();

        // Additional tests for hive partition columns invariants
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_drop_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_drop_column", "SELECT orderkey, orderstatus FROM orders");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN \"$path\"", ".* Cannot drop hidden column");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN orderstatus", "Cannot drop partition column.*");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN orderkey");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN custkey", "Cannot drop the only non-partition column in a table.*");
        assertQuery("SELECT * FROM test_drop_column", "SELECT custkey, orderstatus FROM orders");

        assertUpdate("DROP TABLE test_drop_column");
    }

    @Test
    public void testAvroTypeValidation()
    {
        assertQueryFails("CREATE TABLE test_avro_types (x map(bigint, bigint)) WITH (format = 'AVRO')", "Column 'x' has a non-varchar map key, which is not supported by Avro");
        assertQueryFails("CREATE TABLE test_avro_types (x tinyint) WITH (format = 'AVRO')", "Column 'x' is tinyint, which is not supported by Avro. Use integer instead.");
        assertQueryFails("CREATE TABLE test_avro_types (x smallint) WITH (format = 'AVRO')", "Column 'x' is smallint, which is not supported by Avro. Use integer instead.");

        assertQueryFails("CREATE TABLE test_avro_types WITH (format = 'AVRO') AS SELECT cast(42 AS smallint) z", "Column 'z' is smallint, which is not supported by Avro. Use integer instead.");
    }

    @Test
    public void testOrderByChar()
    {
        assertUpdate("CREATE TABLE char_order_by (c_char char(2))");
        assertUpdate("INSERT INTO char_order_by (c_char) VALUES" +
                "(CAST('a' as CHAR(2)))," +
                "(CAST('a\0' as CHAR(2)))," +
                "(CAST('a  ' as CHAR(2)))", 3);

        MaterializedResult actual = computeActual(getSession(),
                "SELECT * FROM char_order_by ORDER BY c_char ASC");

        assertUpdate("DROP TABLE char_order_by");

        MaterializedResult expected = resultBuilder(getSession(), createCharType(2))
                .row("a\0")
                .row("a ")
                .row("a ")
                .build();

        assertEquals(actual, expected);
    }

    /**
     * Tests correctness of comparison of char(x) and varchar pushed down to a table scan as a TupleDomain
     */
    @Test
    public void testPredicatePushDownToTableScan()
    {
        // Test not specific to Hive, but needs a connector supporting table creation

        assertUpdate("CREATE TABLE test_table_with_char (a char(20))");
        try {
            assertUpdate("INSERT INTO test_table_with_char (a) VALUES" +
                    "(cast('aaa' as char(20)))," +
                    "(cast('bbb' as char(20)))," +
                    "(cast('bbc' as char(20)))," +
                    "(cast('bbd' as char(20)))", 4);

            assertQuery(
                    "SELECT a, a <= 'bbc' FROM test_table_with_char",
                    "VALUES (cast('aaa' as char(20)), true), " +
                            "(cast('bbb' as char(20)), true), " +
                            "(cast('bbc' as char(20)), true), " +
                            "(cast('bbd' as char(20)), false)");

            assertQuery(
                    "SELECT a FROM test_table_with_char WHERE a <= 'bbc'",
                    "VALUES cast('aaa' as char(20)), " +
                            "cast('bbb' as char(20)), " +
                            "cast('bbc' as char(20))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char");
        }
    }

    @DataProvider
    public Object[][] timestampPrecisionAndValues()
    {
        return new Object[][] {
                {HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123")},
                {HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123456")},
                {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123000000")},
                {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123000001")},
                {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123456789")},
                {HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123")},
                {HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123456")},
                {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123000000")},
                {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123000001")},
                {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123456789")}};
    }

    @Test(dataProvider = "timestampPrecisionAndValues")
    public void testParquetTimestampPredicatePushdown(HiveTimestampPrecision timestampPrecision, LocalDateTime value)
    {
        doTestParquetTimestampPredicatePushdown(getSession(), timestampPrecision, value);
    }

    @Test(dataProvider = "timestampPrecisionAndValues")
    public void testParquetTimestampPredicatePushdownOptimizedWriter(HiveTimestampPrecision timestampPrecision, LocalDateTime value)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "experimental_parquet_optimized_writer_enabled", "true")
                .build();
        doTestParquetTimestampPredicatePushdown(session, timestampPrecision, value);
    }

    private void doTestParquetTimestampPredicatePushdown(Session baseSession, HiveTimestampPrecision timestampPrecision, LocalDateTime value)
    {
        Session session = withTimestampPrecision(baseSession, timestampPrecision);
        String tableName = "test_parquet_timestamp_predicate_pushdown_" + randomTableSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (t TIMESTAMP) WITH (format = 'PARQUET')");
        assertUpdate(session, format("INSERT INTO %s VALUES (%s)", tableName, formatTimestamp(value)), 1);
        assertQuery(session, "SELECT * FROM " + tableName, format("VALUES (%s)", formatTimestamp(value)));

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(
                session,
                format("SELECT * FROM %s WHERE t < %s", tableName, formatTimestamp(value)));
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes(), 0);

        queryResult = queryRunner.executeWithQueryId(
                session,
                format("SELECT * FROM %s WHERE t > %s", tableName, formatTimestamp(value)));
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes(), 0);

        assertQueryStats(
                session,
                format("SELECT * FROM %s WHERE t = %s", tableName, formatTimestamp(value)),
                queryStats -> {
                    assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0);
                },
                results -> {});
    }

    @Test(dataProvider = "timestampPrecisionAndValues")
    public void testOrcTimestampPredicatePushdown(HiveTimestampPrecision timestampPrecision, LocalDateTime value)
    {
        Session session = withTimestampPrecision(getSession(), timestampPrecision);
        assertUpdate("DROP TABLE IF EXISTS test_orc_timestamp_predicate_pushdown");
        assertUpdate("CREATE TABLE test_orc_timestamp_predicate_pushdown (t TIMESTAMP) WITH (format = 'ORC')");
        assertUpdate(session, format("INSERT INTO test_orc_timestamp_predicate_pushdown VALUES (%s)", formatTimestamp(value)), 1);
        assertQuery(session, "SELECT * FROM test_orc_timestamp_predicate_pushdown", format("VALUES (%s)", formatTimestamp(value)));

        // to account for the fact that ORC stats are stored at millisecond precision and Trino rounds timestamps,
        // we filter by timestamps that differ from the actual value by at least 1ms, to observe pruning
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(
                session,
                format("SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t < %s", formatTimestamp(value.minusNanos(MILLISECONDS.toNanos(1)))));
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes(), 0);

        queryResult = queryRunner.executeWithQueryId(
                session,
                format("SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t > %s", formatTimestamp(value.plusNanos(MILLISECONDS.toNanos(1)))));
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes(), 0);

        assertQuery(session, "SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t < " + formatTimestamp(value.plusNanos(1)), format("VALUES (%s)", formatTimestamp(value)));

        assertQueryStats(
                session,
                format("SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t = %s", formatTimestamp(value)),
                queryStats -> {
                    assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0);
                },
                results -> {});
    }

    private static String formatTimestamp(LocalDateTime timestamp)
    {
        return format("TIMESTAMP '%s'", TIMESTAMP_FORMATTER.format(timestamp));
    }

    @Test
    public void testParquetShortDecimalPredicatePushdown()
    {
        assertUpdate("DROP TABLE IF EXISTS test_parquet_decimal_predicate_pushdown");
        assertUpdate("CREATE TABLE test_parquet_decimal_predicate_pushdown (decimal_t DECIMAL(5, 3)) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO test_parquet_decimal_predicate_pushdown VALUES DECIMAL '12.345'", 1);

        assertQuery("SELECT * FROM test_parquet_decimal_predicate_pushdown", "VALUES 12.345");
        assertQuery("SELECT count(*) FROM test_parquet_decimal_predicate_pushdown WHERE decimal_t = DECIMAL '12.345'", "VALUES 1");

        assertNoDataRead("SELECT * FROM test_parquet_decimal_predicate_pushdown WHERE decimal_t < DECIMAL '12.345'");
        assertNoDataRead("SELECT * FROM test_parquet_decimal_predicate_pushdown WHERE decimal_t > DECIMAL '12.345'");
        assertNoDataRead("SELECT * FROM test_parquet_decimal_predicate_pushdown WHERE decimal_t != DECIMAL '12.345'");
    }

    @Test
    public void testParquetLongDecimalPredicatePushdown()
    {
        assertUpdate("DROP TABLE IF EXISTS test_parquet_long_decimal_predicate_pushdown");
        assertUpdate("CREATE TABLE test_parquet_long_decimal_predicate_pushdown (decimal_t DECIMAL(20, 3)) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO test_parquet_long_decimal_predicate_pushdown VALUES DECIMAL '12345678900000000.345'", 1);

        assertQuery("SELECT * FROM test_parquet_long_decimal_predicate_pushdown", "VALUES 12345678900000000.345");
        assertQuery("SELECT count(*) FROM test_parquet_long_decimal_predicate_pushdown WHERE decimal_t = DECIMAL '12345678900000000.345'", "VALUES 1");

        assertNoDataRead("SELECT * FROM test_parquet_long_decimal_predicate_pushdown WHERE decimal_t < DECIMAL '12345678900000000.345'");
        assertNoDataRead("SELECT * FROM test_parquet_long_decimal_predicate_pushdown WHERE decimal_t > DECIMAL '12345678900000000.345'");
        assertNoDataRead("SELECT * FROM test_parquet_long_decimal_predicate_pushdown WHERE decimal_t != DECIMAL '12345678900000000.345'");
    }

    @Test
    public void testParquetDictionaryPredicatePushdown()
    {
        testParquetDictionaryPredicatePushdown(getSession());
    }

    @Test
    public void testParquetDictionaryPredicatePushdownWithOptimizedWriter()
    {
        testParquetDictionaryPredicatePushdown(
                Session.builder(getSession())
                        .setCatalogSessionProperty("hive", "experimental_parquet_optimized_writer_enabled", "true")
                        .build());
    }

    private void testParquetDictionaryPredicatePushdown(Session session)
    {
        String tableName = "test_parquet_dictionary_pushdown_" + randomTableSuffix();
        assertUpdate(session, "DROP TABLE IF EXISTS " + tableName);
        assertUpdate(session, "CREATE TABLE " + tableName + " (n BIGINT) WITH (format = 'PARQUET')");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES 1, 1, 2, 2, 4, 4, 5, 5", 8);
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE n = 3");
    }

    private void assertNoDataRead(@Language("SQL") String sql)
    {
        assertQueryStats(
                getSession(),
                sql,
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isEqualTo(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));
    }

    private QueryInfo getQueryInfo(DistributedQueryRunner queryRunner, ResultWithQueryId<MaterializedResult> queryResult)
    {
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryResult.getQueryId());
    }

    @Test
    public void testPartitionPruning()
    {
        assertUpdate("CREATE TABLE test_partition_pruning (v bigint, k varchar) WITH (partitioned_by = array['k'])");
        assertUpdate("INSERT INTO test_partition_pruning (v, k) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'e')", 4);

        try {
            String query = "SELECT * FROM test_partition_pruning WHERE k = 'a'";
            assertQuery(query, "VALUES (1, 'a')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR,
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("a"), EXACTLY),
                                                            new FormattedMarker(Optional.of("a"), EXACTLY)))))));

            query = "SELECT * FROM test_partition_pruning WHERE k IN ('a', 'b')";
            assertQuery(query, "VALUES (1, 'a'), (2, 'b')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR,
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("a"), EXACTLY),
                                                            new FormattedMarker(Optional.of("a"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)))))));

            query = "SELECT * FROM test_partition_pruning WHERE k >= 'b'";
            assertQuery(query, "VALUES (2, 'b'), (3, 'c'), (4, 'e')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR,
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("c"), EXACTLY),
                                                            new FormattedMarker(Optional.of("c"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("e"), EXACTLY),
                                                            new FormattedMarker(Optional.of("e"), EXACTLY)))))));

            query = "SELECT * FROM (" +
                    "    SELECT * " +
                    "    FROM test_partition_pruning " +
                    "    WHERE v IN (1, 2, 4) " +
                    ") t " +
                    "WHERE t.k >= 'b'";
            assertQuery(query, "VALUES (2, 'b'), (4, 'e')");
            assertConstraints(
                    query,
                    ImmutableSet.of(
                            new ColumnConstraint(
                                    "k",
                                    VARCHAR,
                                    new FormattedDomain(
                                            false,
                                            ImmutableSet.of(
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("b"), EXACTLY),
                                                            new FormattedMarker(Optional.of("b"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("c"), EXACTLY),
                                                            new FormattedMarker(Optional.of("c"), EXACTLY)),
                                                    new FormattedRange(
                                                            new FormattedMarker(Optional.of("e"), EXACTLY),
                                                            new FormattedMarker(Optional.of("e"), EXACTLY)))))));
        }
        finally {
            assertUpdate("DROP TABLE test_partition_pruning");
        }
    }

    @Test
    public void testBucketFilteringByInPredicate()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_bucket_filtering " +
                "(bucket_key_1 BIGINT, bucket_key_2 VARCHAR, col3 BOOLEAN) " +
                "WITH (" +
                "bucketed_by = ARRAY[ 'bucket_key_1', 'bucket_key_2' ], " +
                "bucket_count = 11" +
                ") ";

        assertUpdate(createTable);

        assertUpdate(
                "INSERT INTO test_bucket_filtering (bucket_key_1, bucket_key_2, col3) VALUES " +
                        "(1, 'd', true), " +
                        "(2, 'c', null), " +
                        "(3, 'b', false), " +
                        "(4, null, true), " +
                        "(null, 'a', true)",
                5);

        try {
            assertQuery(
                    "SELECT * FROM test_bucket_filtering WHERE bucket_key_1 IN (1, 2) AND bucket_key_2 IN ('b', 'd')",
                    "VALUES (1, 'd', true)");

            assertQuery(
                    "SELECT * FROM test_bucket_filtering WHERE bucket_key_1 IN (1, 2, 5, 6) AND bucket_key_2 IN ('b', 'd', 'x')",
                    "VALUES (1, 'd', true)");

            assertQuery(
                    "SELECT * FROM test_bucket_filtering WHERE (bucket_key_1 IN (1, 2) OR bucket_key_1 IS NULL) AND (bucket_key_2 IN ('a', 'd') OR bucket_key_2 IS NULL)",
                    "VALUES (1, 'd', true), (null, 'a', true)");

            assertQueryReturnsEmptyResult("SELECT * FROM test_bucket_filtering WHERE bucket_key_1 IN (5, 6) AND bucket_key_2 IN ('x', 'y')");

            assertQuery(
                    "SELECT * FROM test_bucket_filtering WHERE bucket_key_1 IN (1, 2, 3) AND bucket_key_2 IN ('b', 'c', 'd') AND col3 = true",
                    "VALUES (1, 'd', true)");

            assertQuery(
                    "SELECT * FROM test_bucket_filtering WHERE bucket_key_1 IN (1, 2) AND bucket_key_2 IN ('c', 'd') AND col3 IS NULL",
                    "VALUES (2, 'c', null)");

            assertQuery(
                    "SELECT * FROM test_bucket_filtering WHERE bucket_key_1 IN (1, 2) AND bucket_key_2 IN ('b', 'c') OR col3 = false",
                    "VALUES (2, 'c', null), (3, 'b', false)");
        }
        finally {
            assertUpdate("DROP TABLE test_bucket_filtering");
        }
    }

    @Test
    public void schemaMismatchesWithDereferenceProjections()
    {
        for (TestingHiveStorageFormat format : getAllTestingHiveStorageFormat()) {
            schemaMismatchesWithDereferenceProjections(format.getFormat());
        }
    }

    private void schemaMismatchesWithDereferenceProjections(HiveStorageFormat format)
    {
        // Verify reordering of subfields between a partition column and a table column is not supported
        // eg. table column: a row(c varchar, b bigint), partition column: a row(b bigint, c varchar)
        try {
            assertUpdate("CREATE TABLE evolve_test (dummy bigint, a row(b bigint, c varchar), d bigint) with (format = '" + format + "', partitioned_by=array['d'])");
            assertUpdate("INSERT INTO evolve_test values (1, row(1, 'abc'), 1)", 1);
            assertUpdate("ALTER TABLE evolve_test DROP COLUMN a");
            assertUpdate("ALTER TABLE evolve_test ADD COLUMN a row(c varchar, b bigint)");
            assertUpdate("INSERT INTO evolve_test values (2, row('def', 2), 2)", 1);
            assertQueryFails("SELECT a.b FROM evolve_test where d = 1", ".*There is a mismatch between the table and partition schemas.*");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS evolve_test");
        }

        // Subfield absent in partition schema is reported as null
        // i.e. "a.c" produces null for rows that were inserted before type of "a" was changed
        try {
            assertUpdate("CREATE TABLE evolve_test (dummy bigint, a row(b bigint), d bigint) with (format = '" + format + "', partitioned_by=array['d'])");
            assertUpdate("INSERT INTO evolve_test values (1, row(1), 1)", 1);
            assertUpdate("ALTER TABLE evolve_test DROP COLUMN a");
            assertUpdate("ALTER TABLE evolve_test ADD COLUMN a row(b bigint, c varchar)");
            assertUpdate("INSERT INTO evolve_test values (2, row(2, 'def'), 2)", 1);
            assertQuery("SELECT a.c FROM evolve_test", "SELECT 'def' UNION SELECT null");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS evolve_test");
        }

        // Verify field access when the row evolves without changes to field type
        try {
            assertUpdate("CREATE TABLE evolve_test (dummy bigint, a row(b bigint, c varchar), d bigint) with (format = '" + format + "', partitioned_by=array['d'])");
            assertUpdate("INSERT INTO evolve_test values (1, row(1, 'abc'), 1)", 1);
            assertUpdate("ALTER TABLE evolve_test DROP COLUMN a");
            assertUpdate("ALTER TABLE evolve_test ADD COLUMN a row(b bigint, c varchar, e int)");
            assertUpdate("INSERT INTO evolve_test values (2, row(2, 'def', 2), 2)", 1);
            assertQuery("SELECT a.b FROM evolve_test", "VALUES 1, 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS evolve_test");
        }
    }

    @Test
    public void testSubfieldReordering()
    {
        // Validate for formats for which subfield access is name based
        List<HiveStorageFormat> formats = ImmutableList.of(HiveStorageFormat.ORC, HiveStorageFormat.PARQUET);

        for (HiveStorageFormat format : formats) {
            // Subfields reordered in the file are read correctly. e.g. if partition column type is row(b bigint, c varchar) but the file
            // column type is row(c varchar, b bigint), "a.b" should read the correct field from the file.
            try {
                assertUpdate("CREATE TABLE evolve_test (dummy bigint, a row(b bigint, c varchar)) with (format = '" + format + "')");
                assertUpdate("INSERT INTO evolve_test values (1, row(1, 'abc'))", 1);
                assertUpdate("ALTER TABLE evolve_test DROP COLUMN a");
                assertUpdate("ALTER TABLE evolve_test ADD COLUMN a row(c varchar, b bigint)");
                assertQuery("SELECT a.b FROM evolve_test", "VALUES 1");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS evolve_test");
            }

            // Assert that reordered subfields are read correctly for a two-level nesting. This is useful for asserting correct adaptation
            // of residue projections in HivePageSourceProvider
            try {
                assertUpdate("CREATE TABLE evolve_test (dummy bigint, a row(b bigint, c row(x bigint, y varchar))) with (format = '" + format + "')");
                assertUpdate("INSERT INTO evolve_test values (1, row(1, row(3, 'abc')))", 1);
                assertUpdate("ALTER TABLE evolve_test DROP COLUMN a");
                assertUpdate("ALTER TABLE evolve_test ADD COLUMN a row(c row(y varchar, x bigint), b bigint)");
                // TODO: replace the following assertion with assertQuery once h2QueryRunner starts supporting row types
                assertQuerySucceeds("SELECT a.c.y, a.c FROM evolve_test");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS evolve_test");
            }
        }
    }

    @Test
    public void testParquetColumnNameMappings()
    {
        Session sessionUsingColumnIndex = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "false")
                .build();
        Session sessionUsingColumnName = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "true")
                .build();

        String tableName = "test_parquet_by_column_index";

        assertUpdate(sessionUsingColumnIndex, format(
                "CREATE TABLE %s(" +
                        "  a varchar, " +
                        "  b varchar) " +
                        "WITH (format='PARQUET')",
                tableName));
        assertUpdate(sessionUsingColumnIndex, "INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);

        assertQuery(
                sessionUsingColumnIndex,
                "SELECT a, b FROM " + tableName,
                "VALUES ('a', 'b')");
        assertQuery(
                sessionUsingColumnIndex,
                "SELECT a FROM " + tableName + " WHERE b = 'b'",
                "VALUES ('a')");

        String tableLocation = (String) computeActual("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName).getOnlyValue();

        // Reverse the table so that the Hive column ordering does not match the Parquet column ordering
        String reversedTableName = "test_parquet_by_column_index_reversed";
        assertUpdate(sessionUsingColumnIndex, format(
                "CREATE TABLE %s(" +
                        "  b varchar, " +
                        "  a varchar) " +
                        "WITH (format='PARQUET', external_location='%s')",
                reversedTableName,
                tableLocation));

        assertQuery(
                sessionUsingColumnIndex,
                "SELECT a, b FROM " + reversedTableName,
                "VALUES ('b', 'a')");
        assertQuery(
                sessionUsingColumnIndex,
                "SELECT a FROM " + reversedTableName + " WHERE b = 'a'",
                "VALUES ('b')");

        assertQuery(
                sessionUsingColumnName,
                "SELECT a, b FROM " + reversedTableName,
                "VALUES ('a', 'b')");
        assertQuery(
                sessionUsingColumnName,
                "SELECT a FROM " + reversedTableName + " WHERE b = 'b'",
                "VALUES ('a')");

        assertUpdate(sessionUsingColumnIndex, "DROP TABLE " + reversedTableName);
        assertUpdate(sessionUsingColumnIndex, "DROP TABLE " + tableName);
    }

    @Test
    public void testParquetWithMissingColumns()
    {
        Session sessionUsingColumnIndex = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "false")
                .build();
        Session sessionUsingColumnName = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "true")
                .build();

        String singleColumnTableName = "test_parquet_with_missing_columns_one";

        assertUpdate(format(
                "CREATE TABLE %s(" +
                        "  a varchar) " +
                        "WITH (format='PARQUET')",
                singleColumnTableName));
        assertUpdate(sessionUsingColumnIndex, "INSERT INTO " + singleColumnTableName + " VALUES ('a')", 1);

        String tableLocation = (String) computeActual("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + singleColumnTableName).getOnlyValue();
        String multiColumnTableName = "test_parquet_missing_columns_two";
        assertUpdate(sessionUsingColumnIndex, format(
                "CREATE TABLE %s(" +
                        "  b varchar, " +
                        "  a varchar) " +
                        "WITH (format='PARQUET', external_location='%s')",
                multiColumnTableName,
                tableLocation));

        assertQuery(
                sessionUsingColumnName,
                "SELECT a FROM " + multiColumnTableName + " WHERE b IS NULL",
                "VALUES ('a')");
        assertQuery(
                sessionUsingColumnName,
                "SELECT a FROM " + multiColumnTableName + " WHERE a = 'a'",
                "VALUES ('a')");

        assertQuery(
                sessionUsingColumnIndex,
                "SELECT b FROM " + multiColumnTableName + " WHERE b = 'a'",
                "VALUES ('a')");
        assertQuery(
                sessionUsingColumnIndex,
                "SELECT b FROM " + multiColumnTableName + " WHERE a IS NULL",
                "VALUES ('a')");

        assertUpdate(sessionUsingColumnIndex, "DROP TABLE " + singleColumnTableName);
        assertUpdate(sessionUsingColumnIndex, "DROP TABLE " + multiColumnTableName);
    }

    @Test
    public void testParquetWithMissingNestedColumns()
    {
        Session sessionUsingColumnIndex = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "false")
                .build();

        Session sessionUsingColumnName = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "true")
                .build();

        String missingNestedFieldsTableName = "test_parquet_missing_nested_fields";

        assertUpdate(format(
                "CREATE TABLE %s(" +
                        "  an_array ARRAY(ROW(a2 int))) " +
                        "WITH (format='PARQUET')",
                missingNestedFieldsTableName));
        assertUpdate(sessionUsingColumnIndex, "INSERT INTO " + missingNestedFieldsTableName + " VALUES (ARRAY[ROW(2)])", 1);

        String tableLocation = (String) computeActual("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + missingNestedFieldsTableName).getOnlyValue();
        String missingNestedArrayTableName = "test_parquet_missing_nested_array";
        assertUpdate(sessionUsingColumnIndex, format(
                "CREATE TABLE %s(" +
                        "  an_array ARRAY(ROW(nested_array ARRAY(varchar), a2 int))) " +
                        "WITH (format='PARQUET', external_location='%s')",
                missingNestedArrayTableName,
                tableLocation));
        /*
         * Expected behavior is to read a null collection when a nested array is not define in the parquet footer.
         * This query should not fail nor an empty collection.
         */
        assertQuery(
                sessionUsingColumnIndex,
                "SELECT an_array[1].nested_array FROM " + missingNestedArrayTableName,
                "VALUES (null)");

        assertQuery(
                sessionUsingColumnName,
                "SELECT an_array[1].nested_array FROM " + missingNestedArrayTableName,
                "VALUES (null)");

        assertUpdate(sessionUsingColumnIndex, "DROP TABLE " + missingNestedFieldsTableName);
        assertUpdate(sessionUsingColumnIndex, "DROP TABLE " + missingNestedArrayTableName);
    }

    @Test
    public void testNestedColumnWithDuplicateName()
    {
        String tableName = "test_nested_column_with_duplicate_name";

        assertUpdate(format(
                "CREATE TABLE %s(" +
                        "  foo varchar, " +
                        "  root ROW (foo varchar)) " +
                        "WITH (format='PARQUET')",
                tableName));
        assertUpdate("INSERT INTO " + tableName + " VALUES ('a', ROW('b'))", 1);
        assertQuery("SELECT root.foo FROM " + tableName + " WHERE foo = 'a'", "VALUES ('b')");
        assertQuery("SELECT root.foo FROM " + tableName + " WHERE root.foo = 'b'", "VALUES ('b')");
        assertQuery("SELECT root.foo FROM " + tableName + " WHERE foo = 'a' AND root.foo = 'b'", "VALUES ('b')");

        assertQuery("SELECT foo FROM " + tableName + " WHERE foo = 'a'", "VALUES ('a')");
        assertQuery("SELECT foo FROM " + tableName + " WHERE root.foo = 'b'", "VALUES ('a')");
        assertQuery("SELECT foo FROM " + tableName + " WHERE foo = 'a' AND root.foo = 'b'", "VALUES ('a')");

        assertTrue(computeActual("SELECT foo FROM " + tableName + " WHERE foo = 'a' AND root.foo = 'a'").getMaterializedRows().isEmpty());
        assertTrue(computeActual("SELECT foo FROM " + tableName + " WHERE foo = 'b' AND root.foo = 'b'").getMaterializedRows().isEmpty());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testParquetNaNStatistics()
    {
        String tableName = "test_parquet_nan_statistics";

        assertUpdate("CREATE TABLE " + tableName + " (c_double DOUBLE, c_real REAL, c_string VARCHAR) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (nan(), cast(nan() as REAL), 'all nan')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (nan(), null, 'null real'), (null, nan(), 'null double')", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES (nan(), 4.2, '4.2 real'), (4.2, nan(), '4.2 double')", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES (0.1, 0.1, 'both 0.1')", 1);

        // These assertions are intended to make sure we are handling NaN values in Parquet statistics,
        // however Parquet file stats created in Trino don't include such values; the test is here mainly to prevent
        // regressions, should a new writer start recording such stats
        assertQuery("SELECT c_string FROM " + tableName + " WHERE c_double > 4", "VALUES ('4.2 double')");
        assertQuery("SELECT c_string FROM " + tableName + " WHERE c_real > 4", "VALUES ('4.2 real')");
    }

    @Test
    public void testMismatchedBucketing()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing16\n" +
                            "WITH (bucket_count = 16, bucketed_by = ARRAY['key16']) AS\n" +
                            "SELECT orderkey key16, comment value16 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing32\n" +
                            "WITH (bucket_count = 32, bucketed_by = ARRAY['key32']) AS\n" +
                            "SELECT orderkey key32, comment value32 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketingN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);

            Session withMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "true")
                    .build();
            Session withoutMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "false")
                    .build();

            @Language("SQL") String writeToTableWithMoreBuckets = "CREATE TABLE test_mismatch_bucketing_out32\n" +
                    "WITH (bucket_count = 32, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";
            @Language("SQL") String writeToTableWithFewerBuckets = "CREATE TABLE test_mismatch_bucketing_out8\n" +
                    "WITH (bucket_count = 8, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";

            assertUpdate(withoutMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(3));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out32");

            assertUpdate(withMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders");

            assertUpdate(withMismatchOptimization, writeToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_mismatch_bucketing_out8", "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing16");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing32");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketingN");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out32");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_out8");
        }
    }

    @Test
    public void testBucketedSelect()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_bucketed_select\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1 FROM orders",
                    15000);
            Session planWithTableNodePartitioning = Session.builder(getSession())
                    .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "true")
                    .build();
            Session planWithoutTableNodePartitioning = Session.builder(getSession())
                    .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "false")
                    .build();

            @Language("SQL") String query = "SELECT count(value1) FROM test_bucketed_select GROUP BY key1";
            @Language("SQL") String expectedQuery = "SELECT count(comment) FROM orders GROUP BY orderkey";

            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(1));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(2));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_bucketed_select");
        }
    }

    @Test
    public void testGroupedExecution()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_grouped_join1\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join2\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                            "SELECT orderkey key2, comment value2 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join3\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                            "SELECT orderkey key3, comment value3 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join4\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key4_bucket']) AS\n" +
                            "SELECT orderkey key4_bucket, orderkey key4_non_bucket, comment value4 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinDual\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['keyD']) AS\n" +
                            "SELECT orderkey keyD, comment valueD FROM orders CROSS JOIN UNNEST(repeat(NULL, 2))",
                    30000);
            assertUpdate(
                    "CREATE TABLE test_grouped_window\n" +
                            "WITH (bucket_count = 5, bucketed_by = ARRAY['key']) AS\n" +
                            "SELECT custkey key, orderkey value FROM orders WHERE custkey <= 5 ORDER BY orderkey LIMIT 10",
                    10);

            // NOT grouped execution; default
            Session notColocated = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "false")
                    .setSystemProperty(GROUPED_EXECUTION, "false")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN with all groups at once, fixed schedule
            Session colocatedAllGroupsAtOnce = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "false")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN, 1 group per worker at a time, fixed schedule
            Session colocatedOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "false")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN with all groups at once, dynamic schedule
            Session colocatedAllGroupsAtOnceDynamic = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Co-located JOIN, 1 group per worker at a time, dynamic schedule
            Session colocatedOneGroupAtATimeDynamic = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();
            // Broadcast JOIN, 1 group per worker at a time
            Session broadcastOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();

            // Broadcast JOIN, 1 group per worker at a time, dynamic schedule
            Session broadcastOneGroupAtATimeDynamic = Session.builder(getSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();

            //
            // HASH JOIN
            // =========

            @Language("SQL") String joinThreeBucketedTable =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key2 = key3";
            @Language("SQL") String joinThreeMixedTable =
                    "SELECT key1, value1, key2, value2, keyN, valueN\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_joinN\n" +
                            "ON key2 = keyN";
            @Language("SQL") String expectedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment FROM orders";
            @Language("SQL") String leftJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM test_grouped_join1\n" +
                            "LEFT JOIN (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "ON key1 = key2";
            @Language("SQL") String rightJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "RIGHT JOIN test_grouped_join1\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedOuterJoinQuery = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN orderkey END, CASE mod(orderkey, 2) WHEN 0 THEN comment END FROM orders";

            assertQuery(notColocated, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(notColocated, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(notColocated, rightJoinBucketedTable, expectedOuterJoinQuery);

            assertQuery(colocatedAllGroupsAtOnce, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnceDynamic, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));

            assertQuery(colocatedAllGroupsAtOnce, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnceDynamic, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));

            //
            // CROSS JOIN and HASH JOIN mixed
            // ==============================

            @Language("SQL") String crossJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "CROSS JOIN (SELECT * FROM test_grouped_join3 WHERE key3 <= 3)";
            @Language("SQL") String expectedCrossJoinQuery =
                    "SELECT key1, value1, key1, value1, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT orderkey key1, comment value1 FROM orders)\n" +
                            "CROSS JOIN\n" +
                            "  (SELECT orderkey key3, comment value3 FROM orders WHERE orderkey <= 3)";
            assertQuery(notColocated, crossJoin, expectedCrossJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));

            //
            // Bucketed and unbucketed HASH JOIN mixed
            // =======================================
            @Language("SQL") String bucketedAndUnbucketedJoin =
                    "SELECT key1, value1, keyN, valueN, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  test_grouped_join1\n" +
                            "JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  JOIN test_grouped_join2\n" +
                            "  ON keyN = key2\n" +
                            ")\n" +
                            "ON key1 = keyN\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key1 = key3";
            @Language("SQL") String expectedBucketedAndUnbucketedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment, orderkey, comment FROM orders";
            assertQuery(notColocated, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));

            //
            // UNION ALL / GROUP BY
            // ====================

            @Language("SQL") String groupBySingleBucketed =
                    "SELECT\n" +
                            "  keyD,\n" +
                            "  count(valueD)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedSingleGroupByQuery = "SELECT orderkey, 2 FROM orders";
            @Language("SQL") String groupByOfUnionBucketed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(value3)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL value3\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL value3\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT key3 key, NULL value1, NULL value2, value3\n" +
                            "  FROM test_grouped_join3\n" +
                            "  WHERE key3 % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String groupByOfUnionMixed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(valueN)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL valueN\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL valueN\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, NULL value1, NULL value2, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  WHERE keyN % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String expectedGroupByOfUnion = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN comment END, CASE mod(orderkey, 3) WHEN 0 THEN comment END FROM orders";
            // In this case:
            // * left side can take advantage of bucketed execution
            // * right side does not have the necessary organization to allow its parent to take advantage of bucketed execution
            // In this scenario, we give up bucketed execution altogether. This can potentially be improved.
            //
            //       AGG(key)
            //           |
            //       UNION ALL
            //      /         \
            //  AGG(key)  Scan (not bucketed)
            //     |
            // Scan (bucketed on key)
            @Language("SQL") String groupByOfUnionOfGroupByMixed =
                    "SELECT\n" +
                            "  key, sum(cnt) cnt\n" +
                            "FROM (\n" +
                            "  SELECT keyD key, count(valueD) cnt\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, 1 cnt\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "group by key";
            @Language("SQL") String expectedGroupByOfUnionOfGroupBy = "SELECT orderkey, 3 FROM orders";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionMixed, expectedGroupByOfUnion, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionOfGroupByMixed, expectedGroupByOfUnionOfGroupBy, assertRemoteExchangesCount(2));

            //
            // GROUP BY and JOIN mixed
            // ========================
            @Language("SQL") String joinGroupedWithGrouped =
                    "SELECT key1, count1, count2\n" +
                            "FROM (\n" +
                            "  SELECT keyD key1, count(valueD) count1\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD key2, count(valueD) count2\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedJoinGroupedWithGrouped = "SELECT orderkey, 2, 2 FROM orders";
            @Language("SQL") String joinGroupedWithUngrouped =
                    "SELECT keyD, countD, valueN\n" +
                            "FROM (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "ON keyD = keyN";
            @Language("SQL") String expectedJoinGroupedWithUngrouped = "SELECT orderkey, 2, comment FROM orders";
            @Language("SQL") String joinUngroupedWithGrouped =
                    "SELECT keyN, valueN, countD\n" +
                            "FROM (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON keyN = keyD";
            @Language("SQL") String expectedJoinUngroupedWithGrouped = "SELECT orderkey, comment, 2 FROM orders";
            @Language("SQL") String groupOnJoinResult =
                    "SELECT keyD, count(valueD), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON keyD=keyN\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedGroupOnJoinResult = "SELECT orderkey, 2, 2 FROM orders";

            @Language("SQL") String groupOnUngroupedJoinResult =
                    "SELECT key4_bucket, count(value4), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_join4\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON key4_non_bucket=keyN\n" +
                            "GROUP BY key4_bucket";
            @Language("SQL") String expectedGroupOnUngroupedJoinResult = "SELECT orderkey, count(*), count(*) FROM orders group by orderkey";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));

            assertQuery(broadcastOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(broadcastOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(2));
            assertQuery(broadcastOneGroupAtATimeDynamic, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(2));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, joinUngroupedWithGrouped, expectedJoinUngroupedWithGrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(4));

            //
            // Outer JOIN (that involves LookupOuterOperator)
            // ==============================================

            // Chain on the probe side to test duplicating OperatorFactory
            @Language("SQL") String chainedOuterJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT * FROM test_grouped_join1 WHERE mod(key1, 2) = 0)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_join2 WHERE mod(key2, 3) = 0)\n" +
                            "ON key1 = key2\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 WHERE mod(key3, 5) = 0)\n" +
                            "ON key2 = key3";
            // Probe is grouped execution, but build is not
            @Language("SQL") String sharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 WHERE mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN WHERE mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN";
            // The preceding test case, which then feeds into another join
            @Language("SQL") String chainedSharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 WHERE mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN WHERE mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 WHERE mod(key3, 5) = 0)\n" +
                            "ON keyN = key3";
            @Language("SQL") String expectedChainedOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0 OR mod(orderkey, 5) = 0";
            @Language("SQL") String expectedSharedBuildOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN comment END,\n" +
                    "  orderkey,\n" +
                    "  comment\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0";

            assertQuery(notColocated, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(colocatedAllGroupsAtOnce, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATimeDynamic, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(notColocated, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult);
            assertQuery(colocatedAllGroupsAtOnce, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, chainedSharedBuildOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATimeDynamic, chainedSharedBuildOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(2));

            //
            // Window function
            // ===============
            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, count(*) OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, n FROM (SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) AS n FROM test_grouped_window) WHERE n <= 2",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            //
            // Filter out all or majority of splits
            // ====================================
            @Language("SQL") String noSplits =
                    "SELECT key1, arbitrary(value1)\n" +
                            "FROM test_grouped_join1\n" +
                            "WHERE \"$bucket\" < 0\n" +
                            "GROUP BY key1";
            @Language("SQL") String joinMismatchedBuckets =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join1\n" +
                            "  WHERE \"$bucket\"=1\n" +
                            ")\n" +
                            "FULL OUTER JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE \"$bucket\"=11\n" +
                            ")\n" +
                            "ON key1=key2";
            @Language("SQL") String expectedNoSplits = "SELECT 1, 'a' WHERE FALSE";
            @Language("SQL") String expectedJoinMismatchedBuckets = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 13) IN (1, 11)";

            assertQuery(notColocated, noSplits, expectedNoSplits);
            assertQuery(colocatedAllGroupsAtOnce, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(notColocated, joinMismatchedBuckets, expectedJoinMismatchedBuckets);
            assertQuery(colocatedAllGroupsAtOnce, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join1");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join2");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join3");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join4");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinN");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinDual");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_window");
        }
    }

    private Consumer<Plan> assertRemoteExchangesCount(int expectedRemoteExchangesCount)
    {
        return assertRemoteExchangesCount(getSession(), expectedRemoteExchangesCount);
    }

    private Consumer<Plan> assertRemoteExchangesCount(Session session, int expectedRemoteExchangesCount)
    {
        return plan -> {
            int actualRemoteExchangesCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == ExchangeNode.Scope.REMOTE)
                    .findAll()
                    .size();
            if (actualRemoteExchangesCount != expectedRemoteExchangesCount) {
                Metadata metadata = getDistributedQueryRunner().getCoordinator().getMetadata();
                String formattedPlan = textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, StatsAndCosts.empty(), session, 0, false);
                throw new AssertionError(format(
                        "Expected [\n%s\n] remote exchanges but found [\n%s\n] remote exchanges. Actual plan is [\n\n%s\n]",
                        expectedRemoteExchangesCount,
                        actualRemoteExchangesCount,
                        formattedPlan));
            }
        };
    }

    private Consumer<Plan> assertLocalRepartitionedExchangesCount(int expectedLocalExchangesCount)
    {
        return plan -> {
            int actualLocalExchangesCount = searchFrom(plan.getRoot())
                    .where(node -> {
                        if (!(node instanceof ExchangeNode)) {
                            return false;
                        }

                        ExchangeNode exchangeNode = (ExchangeNode) node;
                        return exchangeNode.getScope() == ExchangeNode.Scope.LOCAL && exchangeNode.getType() == ExchangeNode.Type.REPARTITION;
                    })
                    .findAll()
                    .size();
            if (actualLocalExchangesCount != expectedLocalExchangesCount) {
                Session session = getSession();
                Metadata metadata = getDistributedQueryRunner().getCoordinator().getMetadata();
                String formattedPlan = textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, StatsAndCosts.empty(), session, 0, false);
                throw new AssertionError(format(
                        "Expected [\n%s\n] local repartitioned exchanges but found [\n%s\n] local repartitioned exchanges. Actual plan is [\n\n%s\n]",
                        expectedLocalExchangesCount,
                        actualLocalExchangesCount,
                        formattedPlan));
            }
        };
    }

    @Test
    public void testRcTextCharDecoding()
    {
        assertUpdate("CREATE TABLE test_table_with_char_rc WITH (format = 'RCTEXT') AS SELECT CAST('khaki' AS CHAR(7)) char_column", 1);
        try {
            assertQuery(
                    "SELECT * FROM test_table_with_char_rc WHERE char_column = 'khaki  '",
                    "VALUES (CAST('khaki' AS CHAR(7)))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char_rc");
        }
    }

    @Test
    public void testInvalidPartitionValue()
    {
        assertUpdate("CREATE TABLE invalid_partition_value (a int, b varchar) WITH (partitioned_by = ARRAY['b'])");
        assertQueryFails(
                "INSERT INTO invalid_partition_value VALUES (4, 'test' || chr(13))",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: 74 65 73 74 0D\\E");
        assertUpdate("DROP TABLE invalid_partition_value");

        assertQueryFails(
                "CREATE TABLE invalid_partition_value (a, b) WITH (partitioned_by = ARRAY['b']) AS SELECT 4, chr(9731)",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: E2 98 83\\E");
    }

    @Test
    public void testShowColumnMetadata()
    {
        String tableName = "test_show_column_table";

        @Language("SQL") String createTable = "CREATE TABLE " + tableName + " (a bigint, b varchar, c double)";

        Session testSession = testSessionBuilder()
                .setIdentity(ofUser("test_access_owner"))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        assertUpdate(createTable);

        // verify showing columns over a table requires SELECT privileges for the table
        assertAccessAllowed("SHOW COLUMNS FROM " + tableName);
        assertAccessDenied(testSession,
                "SHOW COLUMNS FROM " + tableName,
                "Cannot show columns of table .*." + tableName + ".*",
                privilege(tableName, SHOW_COLUMNS));

        @Language("SQL") String getColumnsSql = "" +
                "SELECT lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_name = '" + tableName + "'";
        assertEquals(computeActual(getColumnsSql).getOnlyColumnAsSet(), ImmutableSet.of("a", "b", "c"));

        // verify with no SELECT privileges on table, querying information_schema will return empty columns
        executeExclusively(() -> {
            try {
                getQueryRunner().getAccessControl().deny(privilege(tableName, SELECT_COLUMN));
                assertQueryReturnsEmptyResult(testSession, getColumnsSql);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        });

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRoleAuthorizationDescriptors()
    {
        Session user = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setIdentity(Identity.forUser("user").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();

        assertUpdate("CREATE ROLE test_r_a_d1 IN hive");
        assertUpdate("CREATE ROLE test_r_a_d2 IN hive");
        assertUpdate("CREATE ROLE test_r_a_d3 IN hive");

        // nothing showing because no roles have been granted
        assertQueryReturnsEmptyResult("SELECT * FROM information_schema.role_authorization_descriptors");

        // role_authorization_descriptors is not accessible for a non-admin user, even when it's empty
        assertQueryFails(user, "SELECT * FROM information_schema.role_authorization_descriptors",
                "Access Denied: Cannot select from table information_schema.role_authorization_descriptors");

        assertUpdate("GRANT test_r_a_d1 TO USER user IN hive");
        // user with same name as a role
        assertUpdate("GRANT test_r_a_d2 TO USER test_r_a_d1 IN hive");
        assertUpdate("GRANT test_r_a_d2 TO USER user1 WITH ADMIN OPTION IN hive");
        assertUpdate("GRANT test_r_a_d2 TO USER user2 IN hive");
        assertUpdate("GRANT test_r_a_d2 TO ROLE test_r_a_d1 IN hive");

        // role_authorization_descriptors is not accessible for a non-admin user
        assertQueryFails(user, "SELECT * FROM information_schema.role_authorization_descriptors",
                "Access Denied: Cannot select from table information_schema.role_authorization_descriptors");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors",
                "VALUES " +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'ROLE', 'NO')," +
                        "('test_r_a_d2', null, null, 'user2', 'USER', 'NO')," +
                        "('test_r_a_d2', null, null, 'user1', 'USER', 'YES')," +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'USER', 'NO')," +
                        "('test_r_a_d1', null, null, 'user', 'USER', 'NO')");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors LIMIT 1000000000",
                "VALUES " +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'ROLE', 'NO')," +
                        "('test_r_a_d2', null, null, 'user2', 'USER', 'NO')," +
                        "('test_r_a_d2', null, null, 'user1', 'USER', 'YES')," +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'USER', 'NO')," +
                        "('test_r_a_d1', null, null, 'user', 'USER', 'NO')");

        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM information_schema.role_authorization_descriptors LIMIT 2)",
                "VALUES (2)");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE role_name = 'test_r_a_d2'",
                "VALUES " +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'USER', 'NO')," +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'ROLE', 'NO')," +
                        "('test_r_a_d2', null, null, 'user1', 'USER', 'YES')," +
                        "('test_r_a_d2', null, null, 'user2', 'USER', 'NO')");

        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM information_schema.role_authorization_descriptors WHERE role_name = 'test_r_a_d2' LIMIT 1)",
                "VALUES 1");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee = 'user'",
                "VALUES ('test_r_a_d1', null, null, 'user', 'USER', 'NO')");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee like 'user%'",
                "VALUES " +
                        "('test_r_a_d1', null, null, 'user', 'USER', 'NO')," +
                        "('test_r_a_d2', null, null, 'user2', 'USER', 'NO')," +
                        "('test_r_a_d2', null, null, 'user1', 'USER', 'YES')");

        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee like 'user%' LIMIT 2)",
                "VALUES 2");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee = 'test_r_a_d1'",
                "VALUES " +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'ROLE', 'NO')," +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'USER', 'NO')");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee = 'test_r_a_d1' LIMIT 1",
                "VALUES " +
                        "('test_r_a_d2', null, null, 'test_r_a_d1', 'USER', 'NO')");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee = 'test_r_a_d1' AND grantee_type = 'USER'",
                "VALUES ('test_r_a_d2', null, null, 'test_r_a_d1', 'USER', 'NO')");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee = 'test_r_a_d1' AND grantee_type = 'ROLE'",
                "VALUES ('test_r_a_d2', null, null, 'test_r_a_d1', 'ROLE', 'NO')");

        assertQuery(
                "SELECT * FROM information_schema.role_authorization_descriptors WHERE grantee_type = 'ROLE'",
                "VALUES ('test_r_a_d2', null, null, 'test_r_a_d1', 'ROLE', 'NO')");

        assertUpdate("DROP ROLE test_r_a_d1 IN hive");
        assertUpdate("DROP ROLE test_r_a_d2 IN hive");
        assertUpdate("DROP ROLE test_r_a_d3 IN hive");
    }

    @Test
    public void testShowViews()
    {
        String viewName = "test_show_views";

        Session testSession = testSessionBuilder()
                .setIdentity(ofUser("test_view_access_owner"))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        assertUpdate("CREATE VIEW " + viewName + " AS SELECT abs(1) as whatever");

        String showViews = format("SELECT * FROM information_schema.views WHERE table_name = '%s'", viewName);
        assertQuery(
                format("SELECT table_name FROM information_schema.views WHERE table_name = '%s'", viewName),
                format("VALUES '%s'", viewName));

        executeExclusively(() -> {
            try {
                getQueryRunner().getAccessControl().denyTables(table -> false);
                assertQueryReturnsEmptyResult(testSession, showViews);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        });

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testShowTablePrivileges()
    {
        try {
            assertUpdate("CREATE SCHEMA bar");
            assertUpdate("CREATE TABLE bar.one(t integer)");
            assertUpdate("CREATE TABLE bar.two(t integer)");
            assertUpdate("CREATE VIEW bar.three AS SELECT t FROM bar.one");
            assertUpdate("CREATE SCHEMA foo"); // `foo.two` does not exist. Make sure this doesn't incorrectly show up in listing.

            computeActual("SELECT * FROM information_schema.table_privileges"); // must not fail
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_schema = 'bar'",
                    "VALUES " +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'one', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'one', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'one', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'one', 'UPDATE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'UPDATE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'UPDATE', 'YES', null)");
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_schema = 'bar' AND table_name = 'two'",
                    "VALUES " +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'UPDATE', 'YES', null)");
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_name = 'two'",
                    "VALUES " +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'two', 'UPDATE', 'YES', null)");
            assertQuery(
                    "SELECT * FROM information_schema.table_privileges WHERE table_name = 'three'",
                    "VALUES " +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'SELECT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'DELETE', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'INSERT', 'YES', null)," +
                            "('admin', 'USER', 'hive', 'USER', 'hive', 'bar', 'three', 'UPDATE', 'YES', null)");
        }
        finally {
            computeActual("DROP SCHEMA IF EXISTS foo");
            computeActual("DROP VIEW IF EXISTS bar.three");
            computeActual("DROP TABLE IF EXISTS bar.two");
            computeActual("DROP TABLE IF EXISTS bar.one");
            computeActual("DROP SCHEMA IF EXISTS bar");
        }
    }

    @Test
    public void testCurrentUserInView()
    {
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");
        String testAccountsUnqualifiedName = "test_accounts";
        String testAccountsViewUnqualifiedName = "test_accounts_view";
        String testAccountsViewFullyQualifiedName = format("%s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), testAccountsViewUnqualifiedName);
        assertUpdate(format("CREATE TABLE %s AS SELECT user_name, account_name" +
                "  FROM (VALUES ('user1', 'account1'), ('user2', 'account2'))" +
                "  t (user_name, account_name)", testAccountsUnqualifiedName), 2);
        assertUpdate(format("CREATE VIEW %s AS SELECT account_name FROM test_accounts WHERE user_name = CURRENT_USER", testAccountsViewUnqualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user1", testAccountsViewFullyQualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user2", testAccountsViewFullyQualifiedName));

        Session user1 = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .setIdentity(Identity.forUser("user1").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();

        Session user2 = testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .setIdentity(Identity.forUser("user2").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();

        assertQuery(user1, "SELECT account_name FROM test_accounts_view", "VALUES 'account1'");
        assertQuery(user2, "SELECT account_name FROM test_accounts_view", "VALUES 'account2'");
        assertUpdate("DROP VIEW test_accounts_view");
        assertUpdate("DROP TABLE test_accounts");
    }

    @Test
    public void testCollectColumnStatisticsOnCreateTable()
    {
        String tableName = "test_collect_column_statistics_on_create_table";
        assertUpdate(format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ") " +
                "AS " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00:00.000', VARCHAR 'abc1', CAST('bcd1' AS VARBINARY), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00:00.000', VARCHAR 'abc2', CAST('bcd2' AS VARBINARY), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00:00.000', VARCHAR 'cba1', CAST('dcb1' AS VARBINARY), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00:00.000', VARCHAR 'cba2', CAST('dcb2' AS VARBINARY), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCollectStatisticsOnCreateTableTimestampWithPrecision()
    {
        Session nanosecondsTimestamp = withTimestampPrecision(getSession(), HiveTimestampPrecision.NANOSECONDS);

        String tableName = "test_stats_on_create_timestamp_with_precision";

        try {
            assertUpdate(nanosecondsTimestamp,
                    "CREATE TABLE " + tableName + "(c_timestamp) AS VALUES " +
                            "TIMESTAMP '1988-04-08 02:03:04.111', " +
                            "TIMESTAMP '1988-04-08 02:03:04.115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.119', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111999', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111111', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111999' ",
                    12);

            assertQuery("SHOW STATS FOR " + tableName,
                    "SELECT * FROM VALUES " +
                            "('c_timestamp', null, 9.0, 0.0, null, null, null), " +
                            "(null, null, null, null, 12.0, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCollectColumnStatisticsOnInsert()
    {
        String tableName = "test_collect_column_statistics_on_insert";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_boolean BOOLEAN, " +
                "   c_bigint BIGINT, " +
                "   c_double DOUBLE, " +
                "   c_timestamp TIMESTAMP, " +
                "   c_varchar VARCHAR, " +
                "   c_varbinary VARBINARY, " +
                "   p_varchar VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', VARCHAR 'abc1', CAST('bcd1' AS VARBINARY), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', VARCHAR 'abc2', CAST('bcd2' AS VARBINARY), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', VARCHAR 'cba1', CAST('dcb1' AS VARBINARY), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', VARCHAR 'cba2', CAST('dcb2' AS VARBINARY), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2'), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCollectColumnStatisticsOnInsertToEmptyTable()
    {
        String tableName = "test_collect_column_statistics_empty_table";

        assertUpdate(format("CREATE TABLE %s (col INT)", tableName));

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('col', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        assertUpdate(format("INSERT INTO %s (col) VALUES 50, 100, 1, 200, 2", tableName), 5);

        assertQuery(format("SHOW STATS FOR %s", tableName),
                "SELECT * FROM VALUES " +
                        "('col', null, 5.0, 0.0, null, 1, 200), " +
                        "(null, null, null, null, 5.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCollectColumnStatisticsOnInsertToPartiallyAnalyzedTable()
    {
        String tableName = "test_collect_column_statistics_partially_analyzed_table";

        assertUpdate(format("CREATE TABLE %s (col INT, col2 INT)", tableName));

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('col', 0.0, 0.0, 1.0, null, null, null), " +
                        "('col2', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        assertUpdate(format("ANALYZE %s WITH (columns = ARRAY['col2'])", tableName), 0);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('col', 0.0, 0.0, 1.0, null, null, null), " +
                        "('col2', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        assertUpdate(format("INSERT INTO %s (col, col2) VALUES (50, 49), (100, 99), (1, 0), (200, 199), (2, 1)", tableName), 5);

        assertQuery(format("SHOW STATS FOR %s", tableName),
                "SELECT * FROM VALUES " +
                        "('col', null, 5.0, 0.0, null, 1, 200), " +
                        "('col2', null, 5.0, 0.0, null, 0, 199), " +
                        "(null, null, null, null, 5.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzePropertiesSystemTable()
    {
        assertQuery(
                "SELECT * FROM system.metadata.analyze_properties WHERE catalog_name = 'hive'",
                "SELECT * FROM VALUES " +
                        "('hive', 'partitions', '', 'array(array(varchar))', 'Partitions to be analyzed'), " +
                        "('hive', 'columns', '', 'array(varchar)', 'Columns to be analyzed')");
    }

    @Test
    public void testAnalyzeEmptyTable()
    {
        String tableName = "test_analyze_empty_table";
        assertUpdate(format("CREATE TABLE %s (c_bigint BIGINT, c_varchar VARCHAR(2))", tableName));
        assertUpdate("ANALYZE " + tableName, 0);
    }

    @Test
    public void testInvalidAnalyzePartitionedTable()
    {
        String tableName = "test_invalid_analyze_partitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, format(".*Table 'hive.tpch.%s' does not exist.*", tableName));

        createPartitionedTableForAnalyzeTest(tableName);

        // Test invalid property
        assertQueryFails(format("ANALYZE %s WITH (error = 1)", tableName), ".*'hive' analyze property 'error' does not exist.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = 1)", tableName), "\\QInvalid value for catalog 'hive' analyze property 'partitions': Cannot convert [1] to array(array(varchar))\\E");
        assertQueryFails(format("ANALYZE %s WITH (partitions = NULL)", tableName), "\\QInvalid null value for catalog 'hive' analyze property 'partitions' from [null]\\E");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[NULL])", tableName), ".*Invalid null value in analyze partitions property.*");

        // Test non-existed partition
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10']])", tableName), ".*Partition no longer exists.*");

        // Test partition schema mismatch
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4']])", tableName), "Partition value count does not match partition column count");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10', 'error']])", tableName), "Partition value count does not match partition column count");

        // Drop the partitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInvalidAnalyzeUnpartitionedTable()
    {
        String tableName = "test_invalid_analyze_unpartitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, ".*Table.*does not exist.*");

        createUnpartitionedTableForAnalyzeTest(tableName);

        // Test partition properties on unpartitioned table
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), "Partition list provided but table is not partitioned");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1']])", tableName), "Partition list provided but table is not partitioned");

        // Drop the partitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        String tableName = "test_analyze_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // No column stats after running an empty analyze
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), 0);
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on 3 partitions including a null partition and a duplicate partition
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]])", tableName), 12);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3'), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Drop the partitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzePartitionedTableWithColumnSubset()
    {
        String tableName = "test_analyze_columns_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on 3 partitions including a null partition and a duplicate partition,
        // restricting to just 2 columns (one duplicate)
        assertUpdate(
                format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]], " +
                        "columns = ARRAY['c_timestamp', 'c_varchar', 'c_timestamp'])", tableName), 12);

        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Run analyze again, this time on 2 new columns (for all partitions); the previously computed stats
        // should be preserved
        assertUpdate(
                format("ANALYZE %s WITH (columns = ARRAY['c_bigint', 'c_double'])", tableName), 16);

        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3'), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4'), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeUnpartitionedTable()
    {
        String tableName = "test_analyze_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.375, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7'), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7'), " +
                        "('c_timestamp', null, 10.0, 0.375, null, null, null), " +
                        "('c_varchar', 40.0, 10.0, 0.375, null, null, null), " +
                        "('c_varbinary', 20.0, null, 0.375, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Drop the unpartitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeTableTimestampWithPrecision()
    {
        String catalog = getSession().getCatalog().get();
        Session nanosecondsTimestamp = Session.builder(withTimestampPrecision(getSession(), HiveTimestampPrecision.NANOSECONDS))
                // Disable column statistics collection when creating the table
                .setCatalogSessionProperty(catalog, "collect_column_statistics_on_write", "false")
                .build();
        Session microsecondsTimestamp = withTimestampPrecision(getSession(), HiveTimestampPrecision.MICROSECONDS);
        Session millisecondsTimestamp = withTimestampPrecision(getSession(), HiveTimestampPrecision.MILLISECONDS);

        String tableName = "test_analyze_timestamp_with_precision";

        try {
            assertUpdate(
                    nanosecondsTimestamp,
                    "CREATE TABLE " + tableName + "(c_timestamp) AS VALUES " +
                            "TIMESTAMP '1988-04-08 02:03:04.111', " +
                            "TIMESTAMP '1988-04-08 02:03:04.115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.119', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111999', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111111', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111115', " +
                            "TIMESTAMP '1988-04-08 02:03:04.111111999' ",
                    12);

            assertQuery("SHOW STATS FOR " + tableName,
                    "SELECT * FROM VALUES " +
                            "('c_timestamp', null, null, null, null, null, null), " +
                            "(null, null, null, null, 12.0, null, null)");

            assertUpdate(format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, tableName));
            assertUpdate(nanosecondsTimestamp, "ANALYZE " + tableName, 12);
            assertQuery("SHOW STATS FOR " + tableName,
                    "SELECT * FROM VALUES " +
                            "('c_timestamp', null, 9.0, 0.0, null, null, null), " +
                            "(null, null, null, null, 12.0, null, null)");

            assertUpdate(format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, tableName));
            assertUpdate(microsecondsTimestamp, "ANALYZE " + tableName, 12);
            assertQuery("SHOW STATS FOR " + tableName,
                    "SELECT * FROM VALUES " +
                            "('c_timestamp', null, 7.0, 0.0, null, null, null), " +
                            "(null, null, null, null, 12.0, null, null)");

            assertUpdate(format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, tableName));
            assertUpdate(millisecondsTimestamp, "ANALYZE " + tableName, 12);
            assertQuery("SHOW STATS FOR " + tableName,
                    "SELECT * FROM VALUES " +
                            "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                            "(null, null, null, null, 12.0, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInvalidColumnsAnalyzeTable()
    {
        String tableName = "test_invalid_analyze_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // Specifying a null column name is not cool
        assertQueryFails(
                "ANALYZE " + tableName + " WITH (columns = ARRAY[null])",
                ".*Invalid null value in analyze columns property.*");

        // You must specify valid column names
        assertQueryFails(
                "ANALYZE " + tableName + " WITH (columns = ARRAY['invalid_name'])",
                ".*Invalid columns specified for analysis.*");

        // Column names must be strings
        assertQueryFails(
                "ANALYZE " + tableName + " WITH (columns = ARRAY[42])",
                "\\QInvalid value for catalog 'hive' analyze property 'columns': Cannot convert [ARRAY[42]] to array(varchar)\\E");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeUnpartitionedTableWithColumnSubset()
    {
        String tableName = "test_analyze_columns_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['c_bigint', 'c_double'])", 16);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7'), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7'), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeUnpartitionedTableWithEmptyColumnSubset()
    {
        String tableName = "test_analyze_columns_unpartitioned_table_with_empty_column_subset";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // Clear table stats
        assertUpdate(format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, tableName));

        // No stats before ANALYZE
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY[])", 16);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsPartitionedTable()
    {
        String tableName = "test_drop_stats_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // Run analyze on the entire table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3'), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Drop stats for 2 partitions
        assertUpdate(format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['p2', '7'], ARRAY['p3', '8']])", TPCH_SCHEMA, tableName));

        // Only stats for the specified partitions should be removed
        // no change
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        // [p2, 7] had stats dropped
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");
        // no change
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        // [p3, 8] had stats dropped
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Drop stats for the entire table
        assertUpdate(format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, tableName));

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");

        // All table stats are gone
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsUnpartitionedTable()
    {
        String tableName = "test_drop_all_stats_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.375, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7'), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7'), " +
                        "('c_timestamp', null, 10.0, 0.375, null, null, null), " +
                        "('c_varchar', 40.0, 10.0, 0.375, null, null, null), " +
                        "('c_varbinary', 20.0, null, 0.375, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Drop stats for the entire table
        assertUpdate(format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, tableName));

        // All table stats are gone
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, null, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInvalidDropStats()
    {
        String unpartitionedTableName = "test_invalid_drop_all_stats_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(unpartitionedTableName);
        String partitionedTableName = "test_invalid_drop_all_stats_partitioned_table";
        createPartitionedTableForAnalyzeTest(partitionedTableName);

        assertQueryFails(
                format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['p2', '7']])", TPCH_SCHEMA, unpartitionedTableName),
                "Cannot specify partition values for an unpartitioned table");
        assertQueryFails(
                format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['p2', '7'], NULL])", TPCH_SCHEMA, partitionedTableName),
                "Null partition value");
        assertQueryFails(
                format("CALL system.drop_stats('%s', '%s', ARRAY[])", TPCH_SCHEMA, partitionedTableName),
                "No partitions provided");
        assertQueryFails(
                format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['p2', '7', 'dummy']])", TPCH_SCHEMA, partitionedTableName),
                ".*don't match the number of partition columns.*");
        assertQueryFails(
                format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['WRONG', 'KEY']])", TPCH_SCHEMA, partitionedTableName),
                "Partition '.*' not found");
        assertQueryFails(
                format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['WRONG', 'KEY']])", TPCH_SCHEMA, "non_existing_table"),
                format("Table '%s.non_existing_table' does not exist", TPCH_SCHEMA));

        assertAccessDenied(
                format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, unpartitionedTableName),
                format("Cannot insert into table hive.tpch.%s", unpartitionedTableName),
                privilege(unpartitionedTableName, INSERT_TABLE));

        assertUpdate("DROP TABLE " + unpartitionedTableName);
        assertUpdate("DROP TABLE " + partitionedTableName);
    }

    protected void createPartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, true);
    }

    protected void createUnpartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, false);
    }

    private void createTableForAnalyzeTest(String tableName, boolean partitioned)
    {
        Session defaultSession = getSession();

        // Disable column statistics collection when creating the table
        Session disableColumnStatsSession = Session.builder(defaultSession)
                .setCatalogSessionProperty(defaultSession.getCatalog().get(), "collect_column_statistics_on_write", "false")
                .build();

        assertUpdate(
                disableColumnStatsSession,
                "" +
                        "CREATE TABLE " +
                        tableName +
                        (partitioned ? " WITH (partitioned_by = ARRAY['p_varchar', 'p_bigint'])\n" : " ") +
                        "AS " +
                        "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint " +
                        "FROM ( " +
                        "  VALUES " +
                        // p_varchar = 'p1', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00:00.000', 'abc1', X'bcd1', 'p1', BIGINT '7'), " +
                        "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00:00.000', 'abc2', X'bcd2', 'p1', BIGINT '7'), " +
                        // p_varchar = 'p2', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00:00.000', 'cba1', X'dcb1', 'p2', BIGINT '7'), " +
                        "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00:00.000', 'cba2', X'dcb2', 'p2', BIGINT '7'), " +
                        // p_varchar = 'p3', p_bigint = BIGINT '8'
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (true, BIGINT '3', DOUBLE '4.4', TIMESTAMP '2012-10-10 01:00:00.000', 'bca1', X'cdb1', 'p3', BIGINT '8'), " +
                        "    (false, BIGINT '2', DOUBLE '3.4', TIMESTAMP '2012-10-10 00:00:00.000', 'bca2', X'cdb2', 'p3', BIGINT '8'), " +
                        // p_varchar = NULL, p_bigint = NULL
                        "    (false, BIGINT '7', DOUBLE '7.7', TIMESTAMP '1977-07-07 07:07:00.000', 'efa1', X'efa1', NULL, NULL), " +
                        "    (false, BIGINT '6', DOUBLE '6.7', TIMESTAMP '1977-07-07 07:06:00.000', 'efa2', X'efa2', NULL, NULL), " +
                        "    (false, BIGINT '5', DOUBLE '5.7', TIMESTAMP '1977-07-07 07:05:00.000', 'efa3', X'efa3', NULL, NULL), " +
                        "    (false, BIGINT '4', DOUBLE '4.7', TIMESTAMP '1977-07-07 07:04:00.000', 'efa4', X'efa4', NULL, NULL) " +
                        ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint)", 16);

        if (partitioned) {
            // Create empty partitions
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e1", "9"));
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e2", "9"));
        }
    }

    @Test
    public void testInsertMultipleColumnsFromSameChannel()
    {
        String tableName = "test_insert_multiple_columns_same_channel";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_bigint_1 BIGINT, " +
                "   c_bigint_2 BIGINT, " +
                "   p_varchar_1 VARCHAR, " +
                "   p_varchar_2 VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar_1', 'p_varchar_2'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT 1 c_bigint_1, 1 c_bigint_2, '2' p_varchar_1, '2' p_varchar_2 ", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = '2' AND p_varchar_2 = '2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '1', '1'), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '1', '1'), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null)");

        assertUpdate(format("" +
                "INSERT INTO %s (c_bigint_1, c_bigint_2, p_varchar_1, p_varchar_2) " +
                "SELECT orderkey, orderkey, orderstatus, orderstatus " +
                "FROM orders " +
                "WHERE orderstatus='O' AND orderkey = 15008", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = 'O' AND p_varchar_2 = 'O')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '15008', '15008'), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '15008', '15008'), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateAvroTableWithSchemaUrl()
            throws Exception
    {
        String tableName = "test_create_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        String createTableSql = getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath());
        String expectedShowCreateTable = getAvroCreateTableSql(tableName, schemaFile.toURI().toString());

        assertUpdate(createTableSql);

        try {
            MaterializedResult actual = computeActual("SHOW CREATE TABLE " + tableName);
            assertEquals(actual.getOnlyValue(), expectedShowCreateTable);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    @Test
    public void testAlterAvroTableWithSchemaUrl()
            throws Exception
    {
        testAlterAvroTableWithSchemaUrl(true, true, true);
    }

    protected void testAlterAvroTableWithSchemaUrl(boolean renameColumn, boolean addColumn, boolean dropColumn)
            throws Exception
    {
        String tableName = "test_alter_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        assertUpdate(getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath()));

        try {
            if (renameColumn) {
                assertQueryFails(format("ALTER TABLE %s RENAME COLUMN dummy_col TO new_dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (addColumn) {
                assertQueryFails(format("ALTER TABLE %s ADD COLUMN new_dummy_col VARCHAR", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (dropColumn) {
                assertQueryFails(format("ALTER TABLE %s DROP COLUMN dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    private String getAvroCreateTableSql(String tableName, String schemaFile)
    {
        return format("CREATE TABLE %s.%s.%s (\n" +
                        "   dummy_col varchar,\n" +
                        "   another_dummy_col varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = '%s',\n" +
                        "   format = 'AVRO'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                schemaFile);
    }

    private static File createAvroSchemaFile()
            throws Exception
    {
        File schemaFile = File.createTempFile("avro_single_column-", ".avsc");
        String schema = "{\n" +
                "  \"namespace\": \"io.trino.test\",\n" +
                "  \"name\": \"single_column\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\":\"string_col\", \"type\":\"string\" }\n" +
                "]}";
        asCharSink(schemaFile, UTF_8).write(schema);
        return schemaFile;
    }

    @Test
    public void testCreateOrcTableWithSchemaUrl()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_orc (\n" +
                        "   dummy_col varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = 'dummy.avsc',\n" +
                        "   format = 'ORC'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertQueryFails(createTableSql, "Cannot specify avro_schema_url table property for storage format: ORC");
    }

    @Test
    public void testCtasFailsWithAvroSchemaUrl()
    {
        @Language("SQL") String ctasSqlWithoutData = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT 'dummy_value' as dummy_col WITH NO DATA";

        assertQueryFails(ctasSqlWithoutData, "CREATE TABLE AS not supported when Avro schema url is set");

        @Language("SQL") String ctasSql = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT * FROM (VALUES('a')) t (a)";

        assertQueryFails(ctasSql, "CREATE TABLE AS not supported when Avro schema url is set");
    }

    @Test
    public void testBucketedTablesFailWithAvroSchemaUrl()
    {
        @Language("SQL") String createSql = "CREATE TABLE create_avro (dummy VARCHAR)\n" +
                "WITH (avro_schema_url = 'dummy_schema',\n" +
                "      bucket_count = 2, bucketed_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "Bucketing columns not supported when Avro schema url is set");
    }

    @Test
    public void testPrunePartitionFailure()
    {
        assertUpdate("CREATE TABLE test_prune_failure\n" +
                "WITH (partitioned_by = ARRAY['p']) AS\n" +
                "SELECT 123 x, 'abc' p", 1);

        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM test_prune_failure\n" +
                "WHERE x < 0 AND cast(p AS int) > 0");

        assertUpdate("DROP TABLE test_prune_failure");
    }

    @Test
    public void testTemporaryStagingDirectorySessionProperties()
    {
        String tableName = "test_temporary_staging_directory_session_properties";
        assertUpdate(format("CREATE TABLE %s(i int)", tableName));

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_staging_directory_enabled", "false")
                .build();

        HiveInsertTableHandle hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());

        session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "temporary_staging_directory_enabled", "true")
                .setCatalogSessionProperty("hive", "temporary_staging_directory_path", "/tmp/custom/temporary-${USER}")
                .build();

        hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertNotEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());
        assertTrue(hiveInsertTableHandle.getLocationHandle().getWritePath().toString().startsWith("file:/tmp/custom/temporary-"));

        assertUpdate("DROP TABLE " + tableName);
    }

    private HiveInsertTableHandle getHiveInsertTableHandle(Session session, String tableName)
    {
        Metadata metadata = getDistributedQueryRunner().getCoordinator().getMetadata();
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    QualifiedObjectName objectName = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    Optional<TableHandle> handle = metadata.getTableHandle(transactionSession, objectName);
                    List<ColumnHandle> columns = ImmutableList.copyOf(metadata.getColumnHandles(transactionSession, handle.get()).values());
                    InsertTableHandle insertTableHandle = metadata.beginInsert(transactionSession, handle.get(), columns);
                    HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle.getConnectorHandle();

                    metadata.finishInsert(transactionSession, insertTableHandle, ImmutableList.of(), ImmutableList.of());
                    return hiveInsertTableHandle;
                });
    }

    @Test
    public void testSortedWritingTempStaging()
    {
        String tableName = "test_sorted_writing";
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s " +
                        "WITH (" +
                        "   bucket_count = 7," +
                        "   bucketed_by = ARRAY['shipmode']," +
                        "   sorted_by = ARRAY['shipmode']" +
                        ") AS " +
                        "SELECT * FROM tpch.tiny.lineitem",
                tableName);

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "sorted_writing_enabled", "true")
                .setCatalogSessionProperty("hive", "temporary_staging_directory_enabled", "true")
                .setCatalogSessionProperty("hive", "temporary_staging_directory_path", "/tmp/custom/temporary-${USER}")
                .build();

        assertUpdate(session, createTableSql, 60175L);
        MaterializedResult expected = computeActual("SELECT * FROM tpch.tiny.lineitem");
        MaterializedResult actual = computeActual("SELECT * FROM " + tableName);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUseSortedProperties()
    {
        String tableName = "test_propagate_table_scan_sorting_properties";
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s " +
                        "WITH (" +
                        "   bucket_count = 8," +
                        "   bucketed_by = ARRAY['custkey']," +
                        "   sorted_by = ARRAY['custkey']" +
                        ") AS " +
                        "SELECT * FROM tpch.tiny.customer",
                tableName);
        assertUpdate(createTableSql, 1500L);

        @Language("SQL") String expected = "SELECT custkey FROM customer ORDER BY 1 NULLS FIRST LIMIT 100";
        @Language("SQL") String actual = format("SELECT custkey FROM %s ORDER BY 1 NULLS FIRST LIMIT 100", tableName);
        Session session = getSession();
        assertQuery(session, actual, expected, assertPartialLimitWithPreSortedInputsCount(session, 0));

        session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "propagate_table_scan_sorting_properties", "true")
                .build();
        assertQuery(session, actual, expected, assertPartialLimitWithPreSortedInputsCount(session, 1));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "testCreateTableWithCompressionCodecDataProvider")
    public void testCreateTableWithCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        testWithAllStorageFormats((session, hiveStorageFormat) -> {
            if (isNativeParquetWriter(session, hiveStorageFormat) && compressionCodec == HiveCompressionCodec.LZ4) {
                // TODO (https://github.com/trinodb/trino/issues/9142) Support LZ4 compression with native Parquet writer
                assertThatThrownBy(() -> testCreateTableWithCompressionCodec(session, hiveStorageFormat, compressionCodec))
                        .hasMessage("Unsupported codec: LZ4");
                return;
            }
            testCreateTableWithCompressionCodec(session, hiveStorageFormat, compressionCodec);
        });
    }

    @DataProvider
    public Object[][] testCreateTableWithCompressionCodecDataProvider()
    {
        return Stream.of(HiveCompressionCodec.values())
                .collect(toDataProvider());
    }

    private void testCreateTableWithCompressionCodec(Session session, HiveStorageFormat storageFormat, HiveCompressionCodec compressionCodec)
    {
        session = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "compression_codec", compressionCodec.name())
                .build();
        String tableName = "test_table_with_compression_" + compressionCodec;
        assertUpdate(session, format("CREATE TABLE %s WITH (format = '%s') AS TABLE tpch.tiny.nation", tableName, storageFormat), 25);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSelectWithNoColumns()
    {
        testWithAllStorageFormats(this::testSelectWithNoColumns);
    }

    private void testSelectWithNoColumns(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_select_with_no_columns";
        @Language("SQL") String createTable = format(
                "CREATE TABLE %s (col0) WITH (format = '%s') AS VALUES 5, 6, 7",
                tableName,
                storageFormat);
        assertUpdate(session, createTable, 3);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertQuery("SELECT 1 FROM " + tableName, "VALUES 1, 1, 1");
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testColumnPruning()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "orc_use_column_names", "true")
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "true")
                .build();

        testWithStorageFormat(new TestingHiveStorageFormat(session, HiveStorageFormat.ORC), this::testColumnPruning);
        testWithStorageFormat(new TestingHiveStorageFormat(session, HiveStorageFormat.PARQUET), this::testColumnPruning);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        switch (columnName) {
            case " aleadingspace":
                return "Hive column names must not start with a space: ' aleadingspace'".equals(exception.getMessage());
            case "atrailingspace ":
                return "Hive column names must not end with a space: 'atrailingspace '".equals(exception.getMessage());
            case "a,comma":
                return "Hive column names must not contain commas: 'a,comma'".equals(exception.getMessage());
        }
        return false;
    }

    private void testColumnPruning(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_schema_evolution_column_pruning_" + storageFormat.name().toLowerCase(ENGLISH);
        String evolvedTableName = tableName + "_evolved";

        assertUpdate(session, "DROP TABLE IF EXISTS " + tableName);
        assertUpdate(session, "DROP TABLE IF EXISTS " + evolvedTableName);

        assertUpdate(session, format(
                "CREATE TABLE %s(" +
                        "  a bigint, " +
                        "  b varchar, " +
                        "  c row(" +
                        "    f1 row(" +
                        "      g1 bigint," +
                        "      g2 bigint), " +
                        "    f2 varchar, " +
                        "    f3 varbinary), " +
                        "  d integer) " +
                        "WITH (format='%s')",
                tableName,
                storageFormat));
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (42, 'ala', ROW(ROW(177, 873321), 'ma kota', X'abcdef'), 12345678)", 1);

        // All data
        assertQuery(
                session,
                "SELECT a, b, c.f1.g1, c.f1.g2, c.f2, c.f3, d FROM " + tableName,
                "VALUES (42, 'ala', 177, 873321, 'ma kota', X'abcdef', 12345678)");

        // Pruning
        assertQuery(
                session,
                "SELECT b, c.f1.g2, c.f3, d FROM " + tableName,
                "VALUES ('ala', 873321, X'abcdef', 12345678)");

        String tableLocation = (String) computeActual("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName).getOnlyValue();

        assertUpdate(session, format(
                "CREATE TABLE %s(" +
                        "  e tinyint, " + // added
                        "  a bigint, " +
                        "  bxx varchar, " + // renamed
                        "  c row(" +
                        "    f1 row(" +
                        "      g1xx bigint," + // renamed
                        "      g2 bigint), " +
                        "    f2xx varchar, " + // renamed
                        "    f3 varbinary), " +
                        "  d integer, " +
                        "  f smallint) " + // added
                        "WITH (format='%s', external_location='%s')",
                evolvedTableName,
                storageFormat,
                tableLocation));

        // Pruning being an effected of renamed fields (schema evolution)
        assertQuery(
                session,
                "SELECT a, bxx, c.f1.g1xx, c.f1.g2, c.f2xx, c.f3,  d, e, f FROM " + evolvedTableName + " t",
                "VALUES (42, NULL, NULL, 873321, NULL, X'abcdef', 12345678, NULL, NULL)");

        assertUpdate(session, "DROP TABLE " + evolvedTableName);
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testUnsupportedCsvTable()
    {
        assertQueryFails(
                "CREATE TABLE create_unsupported_csv(i INT, bound VARCHAR(10), unbound VARCHAR, dummy VARCHAR) WITH (format = 'CSV')",
                "\\QHive CSV storage format only supports VARCHAR (unbounded). Unsupported columns: i integer, bound varchar(10)\\E");
    }

    @Test
    public void testWriteInvalidPrecisionTimestamp()
    {
        Session session = withTimestampPrecision(getSession(), HiveTimestampPrecision.MICROSECONDS);
        assertQueryFails(
                session,
                "CREATE TABLE test_invalid_precision_timestamp(ts) AS SELECT TIMESTAMP '2001-02-03 11:22:33.123456789'",
                "\\QIncorrect timestamp precision for timestamp(9); the configured precision is " + HiveTimestampPrecision.MICROSECONDS);
        assertQueryFails(
                session,
                "CREATE TABLE test_invalid_precision_timestamp (ts TIMESTAMP(9))",
                "\\QIncorrect timestamp precision for timestamp(9); the configured precision is " + HiveTimestampPrecision.MICROSECONDS);
        assertQueryFails(
                session,
                "CREATE TABLE test_invalid_precision_timestamp(ts) AS SELECT TIMESTAMP '2001-02-03 11:22:33.123'",
                "\\QIncorrect timestamp precision for timestamp(3); the configured precision is " + HiveTimestampPrecision.MICROSECONDS);
        assertQueryFails(
                session,
                "CREATE TABLE test_invalid_precision_timestamp (ts TIMESTAMP(3))",
                "\\QIncorrect timestamp precision for timestamp(3); the configured precision is " + HiveTimestampPrecision.MICROSECONDS);
    }

    @Test
    public void testOptimize()
    {
        String tableName = "test_optimize_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation WITH NO DATA", 0);

        insertNationNTimes(tableName, 10);
        assertNationNTimes(tableName, 10);

        Set<String> initialFiles = getTableFiles(tableName);
        assertThat(initialFiles).hasSize(10);

        // OPTIMIZE must be explicitly enabled
        assertThatThrownBy(() -> computeActual("ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB')"))
                .hasMessage("OPTIMIZE procedure must be explicitly enabled via non_transactional_optimize_enabled session property");
        assertNationNTimes(tableName, 10);
        assertThat(getTableFiles(tableName)).hasSameElementsAs(initialFiles);

        Session optimizeEnabledSession = optimizeEnabledSession();

        assertUpdate(optimizeEnabledSession, "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB')");
        assertNationNTimes(tableName, 10);

        Set<String> compactedFiles = getTableFiles(tableName);
        // we expect at most 3 files due to write parallelism
        assertThat(compactedFiles).hasSizeLessThanOrEqualTo(3);
        assertThat(intersection(initialFiles, compactedFiles)).isEmpty();

        // compact with low threshold; nothing should change
        assertUpdate(optimizeEnabledSession, "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10B')");
        assertThat(getTableFiles(tableName)).hasSameElementsAs(compactedFiles);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeWithWriterScaling()
    {
        String tableName = "test_optimize_witer_scaling" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation WITH NO DATA", 0);

        insertNationNTimes(tableName, 4);
        assertNationNTimes(tableName, 4);

        Set<String> initialFiles = getTableFiles(tableName);
        assertThat(initialFiles).hasSize(4);

        Session writerScalingSession = Session.builder(optimizeEnabledSession())
                .setSystemProperty("scale_writers", "true")
                .setSystemProperty("writer_min_size", "100GB")
                .build();

        assertUpdate(writerScalingSession, "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB')");
        assertNationNTimes(tableName, 4);

        Set<String> compactedFiles = getTableFiles(tableName);
        assertThat(compactedFiles).hasSize(1);
        assertThat(intersection(initialFiles, compactedFiles)).isEmpty();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeWithPartitioning()
    {
        int insertCount = 4;
        int partitionsCount = 5;

        String tableName = "test_optimize_with_partitioning_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(" +
                "  nationkey BIGINT, " +
                "  name VARCHAR, " +
                "  comment VARCHAR, " +
                "  regionkey BIGINT" +
                ")" +
                "WITH (partitioned_by = ARRAY['regionkey'])");

        insertNationNTimes(tableName, insertCount);
        assertNationNTimes(tableName, insertCount);

        Set<String> initialFiles = getTableFiles(tableName);
        assertThat(initialFiles).hasSize(insertCount * partitionsCount);

        Session optimizeEnabledSession = optimizeEnabledSession();
        Session writerScalingSession = Session.builder(optimizeEnabledSession)
                .setSystemProperty("scale_writers", "true")
                .setSystemProperty("writer_min_size", "100GB")
                .build();

        // optimize with unsupported WHERE
        assertThatThrownBy(() -> computeActual(optimizeEnabledSession, "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB') WHERE nationkey = 1"))
                .hasMessageContaining("Unexpected FilterNode found in plan; probably connector was not able to handle provided WHERE expression");
        assertNationNTimes(tableName, insertCount);
        assertThat(getTableFiles(tableName)).hasSameElementsAs(initialFiles);

        // optimize using predicate on on partition key but not matching any partitions
        assertUpdate(writerScalingSession, "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB') WHERE regionkey > 5");
        assertNationNTimes(tableName, insertCount);
        assertThat(getTableFiles(tableName)).hasSameElementsAs(initialFiles);

        // optimize two partitions; also use positional argument
        assertUpdate(writerScalingSession, "ALTER TABLE " + tableName + " EXECUTE optimize('10kB') WHERE regionkey IN (1,2)");
        assertNationNTimes(tableName, insertCount);
        assertThat(getTableFiles(tableName)).hasSize(2 + 3 * insertCount);

        // optimize one more partition; default file_size_threshold
        assertUpdate(writerScalingSession, "ALTER TABLE " + tableName + " EXECUTE optimize WHERE regionkey > 3");
        assertNationNTimes(tableName, insertCount);
        assertThat(getTableFiles(tableName)).hasSize(3 + 2 * insertCount);

        // optimize remaining partitions
        assertUpdate(writerScalingSession, "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB')");
        assertNationNTimes(tableName, insertCount);

        Set<String> compactedFiles = getTableFiles(tableName);
        assertThat(compactedFiles).hasSize(partitionsCount);
        assertThat(intersection(initialFiles, compactedFiles)).isEmpty();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeWithBucketing()
    {
        int insertCount = 4;
        String tableName = "test_optimize_with_bucketing_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(" +
                "  nationkey BIGINT, " +
                "  name VARCHAR, " +
                "  comment VARCHAR, " +
                "  regionkey BIGINT" +
                ")" +
                "WITH (bucketed_by = ARRAY['regionkey'], bucket_count = 4)");

        insertNationNTimes(tableName, insertCount);
        assertNationNTimes(tableName, insertCount);
        Set<String> initialFiles = getTableFiles(tableName);

        assertThatThrownBy(() -> computeActual(optimizeEnabledSession(), "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB')"))
                .hasMessageMatching("Optimizing bucketed Hive table .* is not supported");

        assertThat(getTableFiles(tableName)).hasSameElementsAs(initialFiles);
        assertNationNTimes(tableName, insertCount);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeHiveInformationSchema()
    {
        assertThatThrownBy(() -> computeActual(optimizeEnabledSession(), "ALTER TABLE information_schema.tables EXECUTE optimize(file_size_threshold => '10kB')"))
                .hasMessage("This connector does not support table procedures");
    }

    @Test
    public void testOptimizeHiveSystemTable()
    {
        String tableName = "test_optimize_system_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(a bigint, b bigint) WITH (partitioned_by = ARRAY['b'])");

        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

        assertThatThrownBy(() -> computeActual(optimizeEnabledSession(), format("ALTER TABLE \"%s$partitions\" EXECUTE optimize(file_size_threshold => '10kB')", tableName)))
                .hasMessage("This connector does not support table procedures");

        assertUpdate("DROP TABLE " + tableName);
    }

    private Session optimizeEnabledSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "non_transactional_optimize_enabled", "true")
                .build();
    }

    private void insertNationNTimes(String tableName, int times)
    {
        assertUpdate("INSERT INTO " + tableName + "(nationkey, name, regionkey, comment) " + join(" UNION ALL ", nCopies(times, "SELECT * FROM tpch.sf1.nation")), times * 25);
    }

    private void assertNationNTimes(String tableName, int times)
    {
        String verifyQuery = join(" UNION ALL ", nCopies(times, "SELECT * FROM nation"));
        assertQuery("SELECT nationkey, name, regionkey, comment FROM " + tableName, verifyQuery);
    }

    private Set<String> getTableFiles(String tableName)
    {
        return computeActual("SELECT DISTINCT \"$path\" FROM " + tableName).getOnlyColumn()
                .map(String.class::cast)
                .collect(toSet());
    }

    @Test
    public void testTimestampPrecisionInsert()
    {
        testWithAllStorageFormats(this::testTimestampPrecisionInsert);
    }

    private void testTimestampPrecisionInsert(Session session, HiveStorageFormat storageFormat)
    {
        if (storageFormat == HiveStorageFormat.AVRO) {
            // Avro timestamps are stored with millisecond precision
            return;
        }

        String tableName = "test_timestamp_precision_" + randomTableSuffix();
        String createTable = "CREATE TABLE " + tableName + " (ts TIMESTAMP) WITH (format = '%s')";
        @Language("SQL") String insert = "INSERT INTO " + tableName + " VALUES (TIMESTAMP '%s')";

        testTimestampPrecisionWrites(
                session,
                tableName,
                (ts, precision) -> {
                    assertUpdate("DROP TABLE IF EXISTS " + tableName);
                    assertUpdate(format(createTable, storageFormat));
                    assertUpdate(withTimestampPrecision(session, precision), format(insert, ts), 1);
                });
    }

    @Test
    public void testTimestampPrecisionCtas()
    {
        testWithAllStorageFormats((session, storageFormat) -> testTimestampPrecisionCtas(session, storageFormat));
    }

    private void testTimestampPrecisionCtas(Session session, HiveStorageFormat storageFormat)
    {
        if (storageFormat == HiveStorageFormat.AVRO) {
            // Avro timestamps are stored with millisecond precision
            return;
        }

        String tableName = "test_timestamp_precision_" + randomTableSuffix();
        String createTableAs = "CREATE TABLE " + tableName + " WITH (format = '%s') AS SELECT TIMESTAMP '%s' ts";

        testTimestampPrecisionWrites(
                session,
                tableName,
                (ts, precision) -> {
                    assertUpdate("DROP TABLE IF EXISTS " + tableName);
                    assertUpdate(withTimestampPrecision(session, precision), format(createTableAs, storageFormat, ts), 1);
                });
    }

    private void testTimestampPrecisionWrites(Session session, String tableName, BiConsumer<String, HiveTimestampPrecision> populateData)
    {
        populateData.accept("2019-02-03 18:30:00.123", HiveTimestampPrecision.MILLISECONDS);
        @Language("SQL") String sql = "SELECT ts FROM " + tableName;
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MILLISECONDS), sql, "VALUES ('2019-02-03 18:30:00.123')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MICROSECONDS), sql, "VALUES ('2019-02-03 18:30:00.123')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.NANOSECONDS), sql, "VALUES ('2019-02-03 18:30:00.123')");

        populateData.accept("2019-02-03 18:30:00.456789", HiveTimestampPrecision.MICROSECONDS);
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MILLISECONDS), sql, "VALUES ('2019-02-03 18:30:00.457')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MICROSECONDS), sql, "VALUES ('2019-02-03 18:30:00.456789')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.NANOSECONDS), sql, "VALUES ('2019-02-03 18:30:00.456789000')");

        populateData.accept("2019-02-03 18:30:00.456789876", HiveTimestampPrecision.NANOSECONDS);
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MILLISECONDS), sql, "VALUES ('2019-02-03 18:30:00.457')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MICROSECONDS), sql, "VALUES ('2019-02-03 18:30:00.456790')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.NANOSECONDS), sql, "VALUES ('2019-02-03 18:30:00.456789876')");

        // some rounding edge cases

        populateData.accept("2019-02-03 18:30:00.999999", HiveTimestampPrecision.MICROSECONDS);
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MILLISECONDS), sql, "VALUES ('2019-02-03 18:30:01.000')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MICROSECONDS), sql, "VALUES ('2019-02-03 18:30:00.999999')");

        populateData.accept("2019-02-03 18:30:00.999999999", HiveTimestampPrecision.NANOSECONDS);
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MILLISECONDS), sql, "VALUES ('2019-02-03 18:30:01.000')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.MICROSECONDS), sql, "VALUES ('2019-02-03 18:30:01.000000')");
        assertQuery(withTimestampPrecision(session, HiveTimestampPrecision.NANOSECONDS), sql, "VALUES ('2019-02-03 18:30:00.999999999')");
    }

    @Test
    public void testSelectFromViewWithoutDefaultCatalogAndSchema()
    {
        String viewName = "select_from_view_without_catalog_and_schema_" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM nation WHERE nationkey=1");
        assertQuery("SELECT count(*) FROM " + viewName, "VALUES 1");
        assertQuery("SELECT count(*) FROM hive.tpch." + viewName, "VALUES 1");
        Session sessionNoCatalog = Session.builder(getSession())
                .setCatalog(Optional.empty())
                .setSchema(Optional.empty())
                .build();
        assertQueryFails(sessionNoCatalog, "SELECT count(*) FROM " + viewName, ".*Schema must be specified when session schema is not set.*");
        assertQuery(sessionNoCatalog, "SELECT count(*) FROM hive.tpch." + viewName, "VALUES 1");
    }

    @Test
    public void testSelectFromPrestoViewReferencingHiveTableWithTimestamps()
    {
        Session defaultSession = getSession();
        Session millisSession = Session.builder(defaultSession)
                .setCatalogSessionProperty("hive", "timestamp_precision", "MILLISECONDS")
                .setCatalogSessionProperty("hive_timestamp_nanos", "timestamp_precision", "MILLISECONDS")
                .build();
        Session nanosSessions = Session.builder(defaultSession)
                .setCatalogSessionProperty("hive", "timestamp_precision", "NANOSECONDS")
                .setCatalogSessionProperty("hive_timestamp_nanos", "timestamp_precision", "NANOSECONDS")
                .build();

        // Hive views tests covered in TestHiveViews.testTimestampHiveView and TestHiveViesLegacy.testTimestampHiveView
        String tableName = "ts_hive_table_" + randomTableSuffix();
        assertUpdate(
                withTimestampPrecision(defaultSession, HiveTimestampPrecision.NANOSECONDS),
                "CREATE TABLE " + tableName + " AS SELECT TIMESTAMP '1990-01-02 12:13:14.123456789' ts",
                1);

        // Presto view created with config property set to MILLIS and session property not set
        String prestoViewNameDefault = "presto_view_ts_default_" + randomTableSuffix();
        assertUpdate(defaultSession, "CREATE VIEW " + prestoViewNameDefault + " AS SELECT *  FROM " + tableName);

        assertThat(query(defaultSession, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(defaultSession, "SELECT ts  FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123456789'")
        assertThat(query(defaultSession, "SELECT ts  FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(millisSession, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");
        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123456789'")
        assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123456789'")
        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        // Presto view created with config property set to MILLIS and session property set to NANOS
        String prestoViewNameNanos = "presto_view_ts_nanos_" + randomTableSuffix();
        assertUpdate(nanosSessions, "CREATE VIEW " + prestoViewNameNanos + " AS SELECT *  FROM " + tableName);

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(defaultSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'")
        assertThat(query(defaultSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(defaultSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123456789'")
        assertThat(query(defaultSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(millisSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'")
        assertThat(query(millisSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'")
        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123456789'")
        assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        // TODO(https://github.com/trinodb/trino/issues/6295) Presto view schema is fixed on creation
        // should be: assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123456789'")
        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");
    }

    @Test(dataProvider = "legalUseColumnNamesProvider")
    public void testUseColumnNames(HiveStorageFormat format, boolean formatUseColumnNames)
    {
        String lowerCaseFormat = format.name().toLowerCase(Locale.ROOT);
        Session.SessionBuilder builder = Session.builder(getSession());
        if (format == HiveStorageFormat.ORC || format == HiveStorageFormat.PARQUET) {
            builder.setCatalogSessionProperty(catalog, lowerCaseFormat + "_use_column_names", String.valueOf(formatUseColumnNames));
        }
        Session admin = builder.build();
        String tableName = format("test_renames_%s_%s_%s", lowerCaseFormat, formatUseColumnNames, randomTableSuffix());
        assertUpdate(admin, format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, state VARCHAR) WITH (format = '%s', partitioned_by = ARRAY['state'])", tableName, format));
        assertUpdate(admin, format("INSERT INTO %s VALUES(111, 'Katy', 57, 'CA')", tableName), 1);
        assertQuery(admin, "SELECT * FROM " + tableName, "VALUES(111, 'Katy', 57, 'CA')");

        assertUpdate(admin, format("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName));

        boolean canSeeOldData = !formatUseColumnNames && !NAMED_COLUMN_ONLY_FORMATS.contains(format);

        String katyValue = canSeeOldData ? "'Katy'" : "null";
        assertQuery(admin, "SELECT * FROM " + tableName, format("VALUES(111, %s, 57, 'CA')", katyValue));

        assertUpdate(admin, format("INSERT INTO %s (id, new_name, age, state) VALUES(333, 'Cary', 35, 'WA')", tableName), 1);
        assertQuery(admin, "SELECT * FROM " + tableName, format("VALUES(111, %s, 57, 'CA'), (333, 'Cary', 35, 'WA')", katyValue));

        assertUpdate(admin, format("ALTER TABLE %s RENAME COLUMN new_name TO old_name", tableName));
        String caryValue = canSeeOldData ? "'Cary'" : null;
        assertQuery(admin, "SELECT * FROM " + tableName, format("VALUES(111, 'Katy', 57, 'CA'), (333, %s, 35, 'WA')", caryValue));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "legalUseColumnNamesProvider")
    public void testUseColumnAddDrop(HiveStorageFormat format, boolean formatUseColumnNames)
    {
        String lowerCaseFormat = format.name().toLowerCase(Locale.ROOT);
        Session.SessionBuilder builder = Session.builder(getSession());
        if (format == HiveStorageFormat.ORC || format == HiveStorageFormat.PARQUET) {
            builder.setCatalogSessionProperty(catalog, lowerCaseFormat + "_use_column_names", String.valueOf(formatUseColumnNames));
        }
        Session admin = builder.build();
        String tableName = format("test_add_drop_%s_%s_%s", lowerCaseFormat, formatUseColumnNames, randomTableSuffix());
        assertUpdate(admin, format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, state VARCHAR) WITH (format = '%s')", tableName, format));
        assertUpdate(admin, format("INSERT INTO %s VALUES(111, 'Katy', 57, 'CA')", tableName), 1);
        assertQuery(admin, "SELECT * FROM " + tableName, "VALUES(111, 'Katy', 57, 'CA')");

        assertUpdate(admin, format("ALTER TABLE %s DROP COLUMN state", tableName));
        assertQuery(admin, "SELECT * FROM " + tableName, format("VALUES(111, 'Katy', 57)"));

        assertUpdate(admin, format("INSERT INTO %s VALUES(333, 'Cary', 35)", tableName), 1);
        assertQuery(admin, "SELECT * FROM " + tableName, "VALUES(111, 'Katy', 57), (333, 'Cary', 35)");

        assertUpdate(admin, format("ALTER TABLE %s ADD COLUMN state VARCHAR", tableName));
        assertQuery(admin, "SELECT * FROM " + tableName, "VALUES(111, 'Katy', 57, 'CA'), (333, 'Cary', 35, null)");

        assertUpdate(admin, format("ALTER TABLE %s DROP COLUMN state", tableName));
        assertQuery(admin, "SELECT * FROM " + tableName, "VALUES(111, 'Katy', 57), (333, 'Cary', 35)");

        assertUpdate(admin, format("ALTER TABLE %s ADD COLUMN new_state VARCHAR", tableName));
        boolean canSeeOldData = !formatUseColumnNames && !NAMED_COLUMN_ONLY_FORMATS.contains(format);
        String katyState = canSeeOldData ? "'CA'" : "null";
        assertQuery(admin, "SELECT * FROM " + tableName, format("VALUES(111, 'Katy', 57, %s), (333, 'Cary', 35, null)", katyState));

        if (formatUseColumnNames) {
            assertUpdate(admin, format("ALTER TABLE %s DROP COLUMN age", tableName));
            assertQuery(admin, "SELECT * FROM " + tableName, format("VALUES(111, 'Katy', %s), (333, 'Cary', null)", katyState));
            assertUpdate(admin, format("ALTER TABLE %s ADD COLUMN age INT", tableName));
            assertQuery(admin, "SELECT * FROM " + tableName, "VALUES(111, 'Katy', null, 57), (333, 'Cary', null, 35)");
        }

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testAutoPurgeProperty()
    {
        String tableName = "test_auto_purge_property";
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s " +
                        "AS " +
                        "SELECT * FROM tpch.tiny.customer",
                tableName);
        assertUpdate(createTableSql, 1500L);

        TableMetadata tableMetadataDefaults = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadataDefaults.getMetadata().getProperties().get(AUTO_PURGE), null);

        assertUpdate("DROP TABLE " + tableName);

        @Language("SQL") String createTableSqlWithAutoPurge = format("" +
                        "CREATE TABLE %s " +
                        "WITH (" +
                        "   auto_purge = true" +
                        ") AS " +
                        "SELECT * FROM tpch.tiny.customer",
                tableName);
        assertUpdate(createTableSqlWithAutoPurge, 1500L);

        TableMetadata tableMetadataWithPurge = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadataWithPurge.getMetadata().getProperties().get(AUTO_PURGE), true);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testExplainAnalyzePhysicalReadWallTime()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT * FROM nation a",
                "'Physical input read time' = \\{duration=.*}");
    }

    private static final Set<HiveStorageFormat> NAMED_COLUMN_ONLY_FORMATS = ImmutableSet.of(HiveStorageFormat.AVRO, HiveStorageFormat.JSON);

    @DataProvider
    public Object[][] legalUseColumnNamesProvider()
    {
        return new Object[][] {
                {HiveStorageFormat.ORC, true},
                {HiveStorageFormat.ORC, false},
                {HiveStorageFormat.PARQUET, true},
                {HiveStorageFormat.PARQUET, false},
                {HiveStorageFormat.AVRO, false},
                {HiveStorageFormat.JSON, false},
                {HiveStorageFormat.RCBINARY, false},
                {HiveStorageFormat.RCTEXT, false},
                {HiveStorageFormat.SEQUENCEFILE, false},
                {HiveStorageFormat.TEXTFILE, false},
        };
    }

    private Session getParallelWriteSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("task_writer_count", "4")
                .build();
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        assertOneNotNullResult(getSession(), query);
    }

    private void assertOneNotNullResult(Session session, @Language("SQL") String query)
    {
        MaterializedResult results = getQueryRunner().execute(session, query).toTestTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }

    private Type canonicalizeType(Type type)
    {
        return TESTING_TYPE_MANAGER.getType(toHiveType(type).getTypeSignature());
    }

    private void assertColumnType(TableMetadata tableMetadata, String columnName, Type expectedType)
    {
        assertEquals(tableMetadata.getColumn(columnName).getType(), canonicalizeType(expectedType));
    }

    private void assertConstraints(@Language("SQL") String query, Set<ColumnConstraint> expected)
    {
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        Set<ColumnConstraint> constraints = getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))
                .getInputTableColumnInfos().stream()
                .findFirst().get()
                .getColumnConstraints();

        assertTrue(constraints.containsAll(expected));
    }

    private void verifyPartition(boolean hasPartition, TableMetadata tableMetadata, List<String> partitionKeys)
    {
        Object partitionByProperty = tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY);
        if (hasPartition) {
            assertEquals(partitionByProperty, partitionKeys);
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                boolean partitionKey = partitionKeys.contains(columnMetadata.getName());
                assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
            }
        }
        else {
            assertNull(partitionByProperty);
        }
    }

    private void rollback()
    {
        throw new RollbackException();
    }

    private static class RollbackException
            extends RuntimeException
    {
    }

    protected void testWithAllStorageFormats(BiConsumer<Session, HiveStorageFormat> test)
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testWithStorageFormat(storageFormat, test);
        }
    }

    private static void testWithStorageFormat(TestingHiveStorageFormat storageFormat, BiConsumer<Session, HiveStorageFormat> test)
    {
        requireNonNull(storageFormat, "storageFormat is null");
        requireNonNull(test, "test is null");
        Session session = storageFormat.getSession();
        try {
            test.accept(session, storageFormat.getFormat());
        }
        catch (Exception | AssertionError e) {
            fail(format("Failure for format %s with properties %s", storageFormat.getFormat(), session.getCatalogProperties()), e);
        }
    }

    private boolean isNativeParquetWriter(Session session, HiveStorageFormat storageFormat)
    {
        return storageFormat == HiveStorageFormat.PARQUET &&
                "true".equals(session.getCatalogProperties("hive").get("experimental_parquet_optimized_writer_enabled"));
    }

    private List<TestingHiveStorageFormat> getAllTestingHiveStorageFormat()
    {
        Session session = getSession();
        String catalog = session.getCatalog().orElseThrow();
        ImmutableList.Builder<TestingHiveStorageFormat> formats = ImmutableList.builder();
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            if (hiveStorageFormat == HiveStorageFormat.CSV) {
                // CSV supports only unbounded VARCHAR type
                continue;
            }
            if (hiveStorageFormat == HiveStorageFormat.PARQUET) {
                formats.add(new TestingHiveStorageFormat(
                        Session.builder(session)
                                .setCatalogSessionProperty(catalog, "experimental_parquet_optimized_writer_enabled", "false")
                                .build(),
                        hiveStorageFormat));
                formats.add(new TestingHiveStorageFormat(
                        Session.builder(session)
                                .setCatalogSessionProperty(catalog, "experimental_parquet_optimized_writer_enabled", "true")
                                .build(),
                        hiveStorageFormat));
                continue;
            }
            formats.add(new TestingHiveStorageFormat(session, hiveStorageFormat));
        }
        return formats.build();
    }

    private JsonCodec<IoPlan> getIoPlanCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(getQueryRunner().getTypeManager())));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(IoPlan.class);
    }

    private static class TestingHiveStorageFormat
    {
        private final Session session;
        private final HiveStorageFormat format;

        TestingHiveStorageFormat(Session session, HiveStorageFormat format)
        {
            this.session = requireNonNull(session, "session is null");
            this.format = requireNonNull(format, "format is null");
        }

        public Session getSession()
        {
            return session;
        }

        public HiveStorageFormat getFormat()
        {
            return format;
        }
    }

    private static class TypeAndEstimate
    {
        public final Type type;
        public final EstimatedStatsAndCost estimate;

        public TypeAndEstimate(Type type, EstimatedStatsAndCost estimate)
        {
            this.type = requireNonNull(type, "type is null");
            this.estimate = requireNonNull(estimate, "estimate is null");
        }
    }

    @DataProvider
    public Object[][] timestampPrecision()
    {
        return new Object[][] {
                {HiveTimestampPrecision.MILLISECONDS},
                {HiveTimestampPrecision.MICROSECONDS},
                {HiveTimestampPrecision.NANOSECONDS}};
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Hive connector does not support column default values");
    }

    private Session withTimestampPrecision(Session session, HiveTimestampPrecision precision)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, "timestamp_precision", precision.name())
                .build();
    }
}
