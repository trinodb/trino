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
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveType;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Storage;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.planprinter.IoPlanPrinter;
import io.trino.sql.planner.planprinter.IoPlanPrinter.ColumnConstraint;
import io.trino.sql.planner.planprinter.IoPlanPrinter.EstimatedStatsAndCost;
import io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedDomain;
import io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedMarker;
import io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedRange;
import io.trino.sql.planner.planprinter.IoPlanPrinter.IoPlan;
import io.trino.sql.planner.planprinter.IoPlanPrinter.IoPlan.TableColumnInfo;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.type.TypeDeserializer;
import org.assertj.core.api.AbstractLongAssert;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URL;
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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.COLOCATED_JOIN;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MAX;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MIN;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MAX;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MIN;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_SIZE;
import static io.trino.SystemSessionProperties.MAX_WRITER_TASK_COUNT;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.SKEWED_PARTITION_MIN_DATA_PROCESSED_REBALANCE_THRESHOLD;
import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_MIN_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.SystemSessionProperties.USE_TABLE_SCAN_NODE_PARTITIONING;
import static io.trino.SystemSessionProperties.WRITER_SCALING_MIN_DATA_PROCESSED;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.plugin.hive.HiveMetadata.TRINO_CREATED_BY;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.TRINO_VERSION_NAME;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.HiveQueryRunner.createBucketedSession;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveStorageFormat.REGEX;
import static io.trino.plugin.hive.HiveTableProperties.AUTO_PURGE;
import static io.trino.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
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
import static io.trino.sql.tree.ExplainType.Type.DISTRIBUTED;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
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
import static org.junit.jupiter.api.Assumptions.abort;

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

    protected static QueryRunner createHiveQueryRunner(HiveQueryRunner.Builder<?> builder)
            throws Exception
    {
        // Use faster compression codec in tests. TODO remove explicit config when default changes
        verify(new HiveConfig().getHiveCompressionCodec() == HiveCompressionOption.GZIP);
        String hiveCompressionCodec = HiveCompressionCodec.ZSTD.name();

        QueryRunner queryRunner = builder
                .addHiveProperty("hive.compression-codec", hiveCompressionCodec)
                .addHiveProperty("hive.allow-register-partition-procedure", "true")
                // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                .addHiveProperty("hive.writer-sort-buffer-size", "1MB")
                // Make weighted split scheduling more conservative to avoid OOMs in test
                .addHiveProperty("hive.minimum-assigned-split-weight", "0.5")
                .addHiveProperty("hive.partition-projection-enabled", "true")
                // This is needed for e2e scale writers test otherwise 50% threshold of
                // bufferSize won't get exceeded for scaling to happen.
                .addExtraProperty("task.max-local-exchange-buffer-size", "32MB")
                // SQL functions
                .addExtraProperty("sql.path", "hive.functions")
                .addExtraProperty("sql.default-function-catalog", "hive")
                .addExtraProperty("sql.default-function-schema", "functions")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .setTpchBucketedCatalogEnabled(true)
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
        return switch (connectorBehavior) {
            case SUPPORTS_MULTI_STATEMENT_WRITES,
                 SUPPORTS_REPORTING_WRITTEN_BYTES -> true; // FIXME: Fails because only allowed with transactional tables
            case SUPPORTS_ADD_FIELD,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_DROP_FIELD,
                 SUPPORTS_MERGE,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_FIELD,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TRUNCATE -> false;
            case SUPPORTS_CREATE_FUNCTION -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void verifySupportsUpdateDeclaration()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_update", "AS SELECT * FROM nation")) {
            assertQueryFails("UPDATE " + table.getName() + " SET nationkey = 100 WHERE regionkey = 2", MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        }
    }

    @Test
    @Override
    public void verifySupportsRowLevelUpdateDeclaration()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_update", "AS SELECT * FROM nation")) {
            assertQueryFails("UPDATE " + table.getName() + " SET nationkey = nationkey * 100 WHERE regionkey = 2", MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        }
    }

    @Override
    protected String createTableForWrites(String createTable)
    {
        return createTable + " WITH (transactional = true)";
    }

    @Override
    protected void verifySelectAfterInsertFailurePermissible(Throwable e)
    {
        assertThat(getStackTraceAsString(e))
                .containsPattern("io.trino.spi.TrinoException: Cannot read from a table tpch.test_insert_select_\\w+ that was modified within transaction, you need to commit the transaction first");
    }

    @Test
    @Override
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateMultipleCondition()
    {
        assertThatThrownBy(super::testUpdateMultipleCondition)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateCaseSensitivity()
    {
        assertThatThrownBy(super::testUpdateCaseSensitivity)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateRowConcurrently()
            throws Exception
    {
        // TODO (https://github.com/trinodb/trino/issues/10518) test this with a TestHiveConnectorTest version that creates ACID tables by default, or in some other way
        assertThatThrownBy(super::testUpdateRowConcurrently)
                .hasMessage("Unexpected concurrent update failure")
                .cause()
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateWithPredicates()
    {
        assertThatThrownBy(super::testUpdateWithPredicates)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateRowType()
    {
        assertThatThrownBy(super::testUpdateRowType)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdateAllValues()
    {
        assertThatThrownBy(super::testUpdateAllValues)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithVarcharPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    public void testRequiredPartitionFilter()
    {
        testRequiredPartitionFilter("[]");
        testRequiredPartitionFilter("[\"tpch\"]");
    }

    private void testRequiredPartitionFilter(String queryPartitionFilterRequiredSchemas)
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

    @Test
    public void testRequiredPartitionFilterInferred()
    {
        testRequiredPartitionFilterInferred("[]");
        testRequiredPartitionFilterInferred("[\"tpch\"]");
    }

    private void testRequiredPartitionFilterInferred(String queryPartitionFilterRequiredSchemas)
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

    @Test
    public void testRequiredPartitionFilterAppliedOnDifferentSchema()
    {
        String schemaName = "schema_" + randomNameSuffix();
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
    public void testIgnoreQueryPartitionFilterRequiredSchemas()
    {
        String schemaName = "test_partition_filter_" + randomNameSuffix();
        String tableName = schemaName + ".test_partition_filter" + randomNameSuffix();

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "query_partition_filter_required", "false")
                .setCatalogSessionProperty("hive", "query_partition_filter_required_schemas", format("[\"%s\"]", schemaName))
                .build();

        assertUpdate(session, "CREATE SCHEMA " + schemaName);
        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, part varchar) WITH (partitioned_by = ARRAY['part'])");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 'test')", 1);

        assertQuerySucceeds(session, "SELECT * FROM " + tableName + " WHERE id = 1");

        assertUpdate(session, "DROP SCHEMA " + schemaName + " CASCADE");
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
    public void testCreateSchemaWithIncorrectLocation()
    {
        String schemaName = "test_create_schema_with_incorrect_location_" + randomNameSuffix();
        String schemaLocation = "s3://bucket";

        assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')"))
                .hasMessageContaining("Invalid location URI");
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
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        // Override because HivePrincipal's username is case-sensitive unlike TrinoPrincipal
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessageContaining("Access Denied: Cannot create schema")
                .hasStackTraceContaining("CREATE SCHEMA");
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
        assertAccessDenied(user, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION user2", "Cannot set authorization for schema test_schema_authorization to USER user2");
        assertAccessDenied(user, "DROP SCHEMA test_schema_authorization", "Cannot drop schema test_schema_authorization");
        assertUpdate(admin, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION user");
        // only admin can change the owner
        assertAccessDenied(user, "ALTER SCHEMA test_schema_authorization SET AUTHORIZATION user2", "Cannot set authorization for schema test_schema_authorization to USER user2");
        assertUpdate(user, "DROP SCHEMA test_schema_authorization"); // new onwer can drop schema

        // switch owner back to user, and then change the owner to ROLE admin from a different catalog to verify roles are relative to the catalog of the schema
        Session userSessionInDifferentCatalog = testSessionBuilder()
                .setIdentity(Identity.forUser("user").withPrincipal(getSession().getIdentity().getPrincipal()).build())
                .build();
        assertUpdate(admin, "CREATE SCHEMA test_schema_authorization");
        assertAccessDenied(
                userSessionInDifferentCatalog,
                "ALTER SCHEMA hive.test_schema_authorization SET AUTHORIZATION user",
                "Cannot set authorization for schema test_schema_authorization to USER user");
        assertAccessDenied(
                userSessionInDifferentCatalog,
                "DROP SCHEMA hive.test_schema_authorization",
                "Cannot drop schema test_schema_authorization");
        assertUpdate(admin, "ALTER SCHEMA hive.test_schema_authorization SET AUTHORIZATION user");
        assertAccessDenied(
                userSessionInDifferentCatalog,
                "ALTER SCHEMA hive.test_schema_authorization SET AUTHORIZATION user",
                "Cannot set authorization for schema test_schema_authorization to USER user");
        // new owner can drop schema
        assertUpdate(userSessionInDifferentCatalog, "DROP SCHEMA hive.test_schema_authorization");
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
        // only admin can change the owner
        assertAccessDenied(
                alice,
                "ALTER TABLE test_table_authorization.foo SET AUTHORIZATION alice",
                "Cannot set authorization for table test_table_authorization.foo to USER alice");
        // alice as new owner can now drop table
        assertUpdate(alice, "DROP TABLE test_table_authorization.foo");

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

        assertUpdate(admin, "CREATE SCHEMA test_table_authorization_role");
        assertUpdate(admin, "CREATE TABLE test_table_authorization_role.foo (col int)");

        // TODO Change assertions once https://github.com/trinodb/trino/issues/5706 is done
        assertAccessDenied(
                alice,
                "ALTER TABLE test_table_authorization_role.foo SET AUTHORIZATION ROLE admin",
                "Cannot set authorization for table test_table_authorization_role.foo to ROLE admin");
        assertAccessDenied(
                alice,
                "DROP TABLE test_table_authorization_role.foo",
                "Cannot drop table test_table_authorization_role.foo");
        assertUpdate(admin, "ALTER TABLE test_table_authorization_role.foo SET AUTHORIZATION alice");
        // Only admin can change the owner
        assertAccessDenied(
                alice,
                "ALTER TABLE test_table_authorization_role.foo SET AUTHORIZATION ROLE admin",
                "Cannot set authorization for table test_table_authorization_role.foo to ROLE admin");
        // new owner can drop table
        assertUpdate(alice, "DROP TABLE test_table_authorization_role.foo");
        assertUpdate(admin, "DROP SCHEMA test_table_authorization_role");
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

        String schema = "test_view_authorization" + randomNameSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view AS SELECT current_user AS user");

        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION admin",
                "Cannot set authorization for view " + schema + ".test_view to USER admin");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        // only admin can change the owner
        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION admin",
                "Cannot set authorization for view " + schema + ".test_view to USER admin");

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

        String schema = "test_view_authorization" + randomNameSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE TABLE " + schema + ".test_table (col int)");
        assertUpdate(admin, "INSERT INTO " + schema + ".test_table VALUES (1)", 1);
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view SECURITY DEFINER AS SELECT * from " + schema + ".test_table");
        assertUpdate(admin, "GRANT SELECT ON " + schema + ".test_view TO alice");
        assertAccessDenied(
                alice,
                "DROP VIEW " + schema + ".test_view",
                "Cannot drop view " + schema + ".test_view");

        assertQuery(alice, "SELECT * FROM " + schema + ".test_view", "VALUES (1)");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        assertQueryFails(alice, "SELECT * FROM " + schema + ".test_view", "Access Denied: Cannot select from table " + schema + ".test_table");

        // only admin can change the owner
        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION admin",
                "Cannot set authorization for view " + schema + ".test_view to USER admin");
        // new owner can drop the view
        assertUpdate(alice, "DROP VIEW " + schema + ".test_view");
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

        String schema = "test_view_authorization" + randomNameSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE TABLE " + schema + ".test_table (col int)");
        assertUpdate(admin, "INSERT INTO " + schema + ".test_table VALUES (1)", 1);
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view SECURITY INVOKER AS SELECT * from " + schema + ".test_table");
        assertUpdate(admin, "GRANT SELECT ON " + schema + ".test_view TO alice");
        assertAccessDenied(
                alice,
                "DROP VIEW " + schema + ".test_view",
                "Cannot drop view " + schema + ".test_view");

        assertQueryFails(alice, "SELECT * FROM " + schema + ".test_view", "Access Denied: Cannot select from table " + schema + ".test_table");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        assertQueryFails(alice, "SELECT * FROM " + schema + ".test_view", "Access Denied: Cannot select from table " + schema + ".test_table");

        // only admin can change the owner
        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION admin",
                "Cannot set authorization for view " + schema + ".test_view to USER admin");
        // new owner can drop the view
        assertUpdate(alice, "DROP VIEW " + schema + ".test_view");
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

        String schema = "test_view_authorization" + randomNameSuffix();

        assertUpdate(admin, "CREATE SCHEMA " + schema);
        assertUpdate(admin, "CREATE TABLE " + schema + ".test_table (col int)");
        assertUpdate(admin, "CREATE VIEW " + schema + ".test_view AS SELECT * FROM " + schema + ".test_table");

        // TODO Change assertions once https://github.com/trinodb/trino/issues/5706 is done
        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION ROLE admin",
                "Cannot set authorization for view " + schema + ".test_view to ROLE admin");
        assertUpdate(admin, "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION alice");
        // only admin can change the owner
        assertAccessDenied(
                alice,
                "ALTER VIEW " + schema + ".test_view SET AUTHORIZATION ROLE admin",
                "Cannot set authorization for view " + schema + ".test_view to ROLE admin");

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
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_io_explain"),
                                new IoPlanPrinter.Constraint(
                                        false,
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
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY))))))),
                                estimate)),
                Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_io_explain")),
                estimate));

        assertUpdate("DROP TABLE test_io_explain");

        // Test IO explain with large number of discrete components where Domain::simpify comes into play.
        computeActual("CREATE TABLE test_io_explain WITH (partitioned_by = ARRAY['orderkey']) AS SELECT custkey, orderkey FROM orders WHERE orderkey < 200");

        estimate = new EstimatedStatsAndCost(55.0, 990.0, 990.0, 0.0, 0.0);
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_io_explain SELECT custkey, orderkey + 10 FROM test_io_explain WHERE custkey <= 10");
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_io_explain"),
                                new IoPlanPrinter.Constraint(
                                        false,
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
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY))))))),
                                estimate)),
                Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_io_explain")),
                estimate));

        EstimatedStatsAndCost finalEstimate = new EstimatedStatsAndCost(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        estimate = new EstimatedStatsAndCost(1.0, 18.0, 18, 0.0, 0.0);
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_io_explain SELECT custkey, orderkey FROM test_io_explain WHERE orderkey = 100");
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_io_explain"),
                                new IoPlanPrinter.Constraint(
                                        false,
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "orderkey",
                                                        BIGINT,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("100"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("100"), EXACTLY))))))),
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
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_io_explain_column_filters"),
                                new IoPlanPrinter.Constraint(
                                        false,
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
                                                        createVarcharType(1),
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("P"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("P"), EXACTLY))))))),
                                estimate)),
                Optional.empty(),
                finalEstimate));
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT custkey, orderkey, orderstatus FROM test_io_explain_column_filters WHERE custkey <= 10 and (orderstatus='P' or orderstatus='S')");
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_io_explain_column_filters"),
                                new IoPlanPrinter.Constraint(
                                        false,
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
                                                        createVarcharType(1),
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
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY))))))),
                                estimate)),
                Optional.empty(),
                finalEstimate));
        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT custkey, orderkey, orderstatus FROM test_io_explain_column_filters WHERE custkey <= 10 and cast(orderstatus as integer) = 5");
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_io_explain_column_filters"),
                                new IoPlanPrinter.Constraint(
                                        false,
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
                                                                                new FormattedMarker(Optional.of("10"), EXACTLY))))))),
                                estimate)),
                Optional.empty(),
                finalEstimate));

        assertUpdate("DROP TABLE test_io_explain_column_filters");
    }

    @Test
    public void testIoExplainWithEmptyPartitionedTable()
    {
        // Test IO explain a partitioned table with no data.
        assertUpdate("CREATE TABLE test_io_explain_with_empty_partitioned_table WITH (partitioned_by = ARRAY['orderkey']) AS SELECT custkey, orderkey FROM orders WITH NO DATA", 0);

        EstimatedStatsAndCost estimate = new EstimatedStatsAndCost(0.0, 0.0, 0.0, 0.0, 0.0);
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT custkey, orderkey FROM test_io_explain_with_empty_partitioned_table");
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_io_explain_with_empty_partitioned_table"),
                                new IoPlanPrinter.Constraint(true, ImmutableSet.of()),
                                estimate)),
                Optional.empty(),
                estimate));

        assertUpdate("DROP TABLE test_io_explain_with_empty_partitioned_table");
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
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "io_explain_test_no_filter"),
                                new IoPlanPrinter.Constraint(
                                        false,
                                        ImmutableSet.of(
                                                new ColumnConstraint(
                                                        "ds",
                                                        VARCHAR,
                                                        new FormattedDomain(
                                                                false,
                                                                ImmutableSet.of(
                                                                        new FormattedRange(
                                                                                new FormattedMarker(Optional.of("a"), EXACTLY),
                                                                                new FormattedMarker(Optional.of("a"), EXACTLY))))))),
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
        assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))).isEqualTo(new IoPlan(
                ImmutableSet.of(
                        new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "io_explain_test_filter_on_agg"),
                                new IoPlanPrinter.Constraint(
                                        false,
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
                                                                                new FormattedMarker(Optional.of("b"), EXACTLY))))))),
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
            String tableName = "test_types_table_" + randomNameSuffix();
            @Language("SQL") String query = format(
                    "CREATE TABLE %s WITH (partitioned_by = ARRAY['my_col']) AS " +
                            "SELECT 'foo' my_non_partition_col, CAST('%s' AS %s) my_col",
                    tableName,
                    entry.getKey(),
                    type.getDisplayName());

            assertUpdate(query, 1);

            assertThat(getIoPlanCodec().fromJson((String) getOnlyElement(computeActual("EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM " + tableName).getOnlyColumnAsSet())))
                    .describedAs(format("%d) Type %s ", index, type))
                    .isEqualTo(new IoPlan(
                            ImmutableSet.of(new TableColumnInfo(
                                    new CatalogSchemaTableName(catalog, "tpch", tableName),
                                    new IoPlanPrinter.Constraint(
                                            false,
                                            ImmutableSet.of(
                                                    new ColumnConstraint(
                                                            "my_col",
                                                            type,
                                                            new FormattedDomain(
                                                                    false,
                                                                    ImmutableSet.of(
                                                                            new FormattedRange(
                                                                                    new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY),
                                                                                    new FormattedMarker(Optional.of(entry.getKey().toString()), EXACTLY))))))),
                                    estimate)),
                            Optional.empty(),
                            estimate));

            assertUpdate("DROP TABLE " + tableName);
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
        String tableName = "test_types_table_" + randomNameSuffix();
        @Language("SQL") String query = "" +
                "CREATE TABLE " + tableName + " AS " +
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

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM " + tableName).toTestTypes();
        assertThat(results.getRowCount()).isEqualTo(1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertThat(row.getField(0)).isEqualTo("foo");
        assertThat(row.getField(1)).isEqualTo("bar".getBytes(UTF_8));
        assertThat(row.getField(2)).isEqualTo(1L);
        assertThat(row.getField(3)).isEqualTo(2);
        assertThat(row.getField(4)).isEqualTo(3.14);
        assertThat(row.getField(5)).isEqualTo(true);
        assertThat(row.getField(6)).isEqualTo(LocalDate.of(1980, 5, 7));
        assertThat(row.getField(7)).isEqualTo(LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertThat(row.getField(8)).isEqualTo(new BigDecimal("3.14"));
        assertThat(row.getField(9)).isEqualTo(new BigDecimal("12345678901234567890.0123456789"));
        assertThat(row.getField(10)).isEqualTo("bar       ");
        assertUpdate("DROP TABLE " + tableName);

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.columns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertThat(columnMetadata.getExtraInfo()).isEqualTo(columnExtraInfo(partitionKey));
        }

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));
        assertColumnType(tableMetadata, "_partition_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_partition_varchar", createVarcharType(65535));

        MaterializedResult result = computeActual("SELECT * FROM test_partitioned_table");
        assertThat(result.getRowCount()).isEqualTo(0);

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

        assertThat(getQueryRunner().tableExists(session, "test_partitioned_table")).isFalse();
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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, partitionedBy);

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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

        assertColumnType(tableMetadata, "_varchar", createVarcharType(3));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        // assure reader supports basic column reordering and pruning
        assertQuery(session, "SELECT _integer, _varchar, _integer FROM test_format_table", "SELECT 2, 'foo', 2");

        assertQuery(session, "SELECT * FROM test_format_table", select);

        assertUpdate(session, "DROP TABLE test_format_table");

        assertThat(getQueryRunner().tableExists(session, "test_format_table")).isFalse();
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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertThat(partitions).hasSize(3);

        assertQuery(session, "SELECT * FROM test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as");

        assertThat(getQueryRunner().tableExists(session, "test_create_partitioned_table_as")).isFalse();
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
        // We use TEXTFILE in this test because is has a very consistent and predictable size
        @Language("SQL") String createTableSql = "CREATE TABLE test_max_file_size WITH (format = 'TEXTFILE') AS SELECT * FROM tpch.sf1.lineitem LIMIT 1000000";
        @Language("SQL") String selectFileInfo = "SELECT distinct \"$path\", \"$file_size\" FROM test_max_file_size";

        // verify the default behavior is one file per node
        Session session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .setSystemProperty("scale_writers", "false")
                .setSystemProperty("redistribute_writes", "false")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
        assertUpdate(session, createTableSql, 1000000);
        assertThat(computeActual(selectFileInfo).getRowCount()).isEqualTo(1);
        assertUpdate("DROP TABLE test_max_file_size");

        // Write table with small limit and verify we get multiple files per node near the expected size
        // Writer writes chunks of rows that are about 1MB
        DataSize maxSize = DataSize.of(1, MEGABYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .setCatalogSessionProperty("hive", "target_max_file_size", maxSize.toString())
                .build();

        assertUpdate(session, createTableSql, 1000000);
        MaterializedResult result = computeActual(selectFileInfo);
        assertThat(result.getRowCount()).isGreaterThan(1);
        for (MaterializedRow row : result) {
            // allow up to a larger delta due to the very small max size and the relatively large writer chunk size
            assertThat((Long) row.getField(1)).isLessThan(maxSize.toBytes() * 3);
        }

        assertUpdate("DROP TABLE test_max_file_size");
    }

    @Test
    public void testTargetMaxFileSizePartitioned()
    {
        // We use TEXTFILE in this test because is has a very consistent and predictable size
        @Language("SQL") String createTableSql = "" +
                "CREATE TABLE test_max_file_size_partitioned WITH (partitioned_by = ARRAY['returnflag'], format = 'TEXTFILE') AS " +
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.sf1.lineitem LIMIT 1000000";
        @Language("SQL") String selectFileInfo = "SELECT distinct \"$path\", \"$file_size\" FROM test_max_file_size_partitioned";

        // verify the default behavior is one file per node per partition
        Session session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .setSystemProperty("task_max_writer_count", "1")
                // task scale writers should be disabled since we want to write a single file
                .setSystemProperty("task_scale_writers_enabled", "false")
                .setSystemProperty("scale_writers", "false")
                .setSystemProperty("redistribute_writes", "false")
                .setSystemProperty("use_preferred_write_partitioning", "false")
                .build();
        assertUpdate(session, createTableSql, 1000000);
        assertThat(computeActual(selectFileInfo).getRowCount()).isEqualTo(3);
        assertUpdate("DROP TABLE test_max_file_size_partitioned");

        // Write table with small limit and verify we get multiple files per node near the expected size
        // Writer writes chunks of rows that are about 1MB
        DataSize maxSize = DataSize.of(1, MEGABYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .setSystemProperty("task_max_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .setSystemProperty("use_preferred_write_partitioning", "false")
                .setCatalogSessionProperty("hive", "target_max_file_size", maxSize.toString())
                .build();

        assertUpdate(session, createTableSql, 1000000);
        MaterializedResult result = computeActual(selectFileInfo);
        assertThat(result.getRowCount()).isGreaterThan(3);
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
        assertQuery("SELECT \"orc.bloom.filter.columns\", \"orc.bloom.filter.fpp\", trino_query_id, trino_version, transactional FROM \"test_show_properties$properties\"",
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

    @Test
    public void testCreateTableNonSupportedVarcharColumn()
    {
        assertThatThrownBy(() -> {
            assertUpdate("CREATE TABLE test_create_table_non_supported_varchar_column (apple varchar(65536))");
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Unsupported Hive type: varchar\\(65536\\)\\. Supported VARCHAR types: VARCHAR\\(<=65535\\), VARCHAR\\.");
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

    private void testEmptyBucketedTable(Session baseSession, HiveStorageFormat storageFormat, boolean createEmpty)
    {
        String tableName = "test_empty_bucketed_table" + randomNameSuffix();

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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

        assertThat(tableMetadata.metadata().getProperties().get(PARTITIONED_BY_PROPERTY)).isNull();
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKETED_BY_PROPERTY, ImmutableList.of("bucket_key"));
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKET_COUNT_PROPERTY, 11);

        assertThat(computeActual("SELECT * from " + tableName).getRowCount()).isEqualTo(0);

        // make sure that we will get one file per bucket regardless of writer count configured
        Session session = Session.builder(baseSession)
                .setSystemProperty("task_min_writer_count", "4")
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c0')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('a1', 'b1', 'c1')", 1);

        assertQuery("SELECT * from " + tableName, "VALUES ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1')");

        assertUpdate(session, "DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
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
        String tableName = "test_bucketed_table" + randomNameSuffix();

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
        Session parallelWriter = Session.builder(getParallelWriteSession(session))
                .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                .build();
        assertUpdate(parallelWriter, createTable, 3);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

        assertThat(tableMetadata.metadata().getProperties().get(PARTITIONED_BY_PROPERTY)).isNull();
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKETED_BY_PROPERTY, ImmutableList.of("bucket_key"));
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKET_COUNT_PROPERTY, 11);

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
        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
    }

    @Test
    public void testFilterOnBucketedTable()
    {
        testFilterOnBucketedValue(
                "BOOLEAN",
                Stream.concat(IntStream.range(0, 100).mapToObj(i -> "true"), IntStream.range(0, 100).mapToObj(i -> "false"))
                        .collect(toImmutableList()),
                "true",
                100,
                100);

        testFilterOnBucketedValue(
                "TINYINT",
                IntStream.range(0, 127).mapToObj(String::valueOf).collect(toImmutableList()),
                "126",
                26,
                1);

        testFilterOnBucketedValue(
                "SMALLINT",
                IntStream.range(0, 1000).map(i -> i + 22767).mapToObj(String::valueOf).collect(toImmutableList()),
                "22767",
                200,
                1);

        testFilterOnBucketedValue(
                "INTEGER",
                IntStream.range(0, 1000).map(i -> i + 1274942432).mapToObj(String::valueOf).collect(toImmutableList()),
                "1274942432",
                200,
                1);

        testFilterOnBucketedValue(
                "BIGINT",
                IntStream.range(0, 1000).mapToLong(i -> i + 312739231274942432L).mapToObj(String::valueOf).collect(toImmutableList()),
                "312739231274942432",
                200,
                1);

        testFilterOnBucketedValue(
                "REAL",
                IntStream.range(0, 1000).mapToDouble(i -> i + 567.123).mapToObj(val -> "REAL '" + val + "'").collect(toImmutableList()),
                "567.123",
                201,
                1);

        testFilterOnBucketedValue(
                "DOUBLE",
                IntStream.range(0, 1000).mapToDouble(i -> i + 1234567890123.123).mapToObj(val -> "DOUBLE '" + val + "'").collect(toImmutableList()),
                "1234567890123.123",
                201,
                1);

        testFilterOnBucketedValue(
                "VARCHAR",
                IntStream.range(0, 1000).mapToObj(i -> "'test value " + i + "'").collect(toImmutableList()),
                "'test value 5'",
                200,
                1);

        testFilterOnBucketedValue(
                "VARCHAR(20)",
                IntStream.range(0, 1000).mapToObj(i -> "'test value " + i + "'").collect(toImmutableList()),
                "'test value 5'",
                200,
                1);

        testFilterOnBucketedValue(
                "DATE",
                IntStream.range(0, 1000).mapToObj(i -> "DATE '2020-02-12' + interval '" + i + "' day").collect(toImmutableList()),
                "DATE '2020-02-15'",
                200,
                1);

        testFilterOnBucketedValue(
                "ARRAY<INT>",
                IntStream.range(0, 1000)
                        .mapToObj(i -> format("ARRAY[%s, %s, %s, %s]", i + 22767, i + 22768, i + 22769, i + 22770))
                        .collect(toImmutableList()),
                "ARRAY[22767, 22768, 22769, 22770]",
                200,
                1);

        testFilterOnBucketedValue(
                "MAP<DOUBLE, INT>",
                IntStream.range(0, 1000)
                        .mapToObj(i -> format("MAP(ARRAY[%s, %s], ARRAY[%s, %s])", i + 567.123, i + 568.456, i + 22769, i + 22770))
                        .collect(toImmutableList()),
                "MAP(ARRAY[567.123, 568.456], ARRAY[22769, 22770])",
                149,
                1);
    }

    private void testFilterOnBucketedValue(String typeName, List<String> valueList, String filterValue, long expectedPhysicalInputRows, long expectedResult)
    {
        String tableName = "test_filter_on_bucketed_table_" + randomNameSuffix();
        assertUpdate(
                """
                CREATE TABLE %s (bucket_key %s, other_data double)
                WITH (
                    format = 'TEXTFILE',
                    bucketed_by = ARRAY[ 'bucket_key' ],
                    bucket_count = 5)
                """.formatted(tableName, typeName));

        String values = valueList.stream()
                .map(value -> "(" + value + ", rand())")
                .collect(joining(", "));
        assertUpdate("INSERT INTO " + tableName + " VALUES " + values, valueList.size());

        // It will only read data from a single bucket instead of all buckets,
        // so physicalInputPositions should be less than number of rows inserted (.
        assertQueryStats(
                getSession(),
                """
                SELECT count(*)
                FROM %s
                WHERE bucket_key = %s
                """.formatted(tableName, filterValue),
                queryStats -> assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(expectedPhysicalInputRows),
                result -> assertThat(result.getOnlyValue()).isEqualTo(expectedResult));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testBucketedTableUnsupportedTypes()
    {
        testBucketedTableUnsupportedTypes("VARBINARY");
        testBucketedTableUnsupportedTypes("TIMESTAMP");
        testBucketedTableUnsupportedTypes("DECIMAL(10,3)");
        testBucketedTableUnsupportedTypes("CHAR");
        testBucketedTableUnsupportedTypes("ROW(id VARCHAR)");
    }

    private void testBucketedTableUnsupportedTypes(String typeName)
    {
        String tableName = "test_bucketed_table_for_unsupported_types_" + randomNameSuffix();
        assertThatThrownBy(() -> assertUpdate(
                """
                CREATE TABLE %s (bucket_key %s, other_data double)
                WITH (
                    bucketed_by = ARRAY[ 'bucket_key' ],
                    bucket_count = 5)
                """.formatted(tableName, typeName)))
                .hasMessage("Cannot create a table bucketed on an unsupported type");
    }

    /**
     * Regression test for https://github.com/trinodb/trino/issues/5295
     */
    @Test
    public void testBucketedTableWithTimestampColumn()
    {
        String tableName = "test_bucketed_table_with_timestamp_" + randomNameSuffix();

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
        String tableName = "test_create_partitioned_bucketed_table_as_few_rows" + randomNameSuffix();

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
                Session.builder(getParallelWriteSession(session))
                        .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                        .build(),
                createTable,
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
    }

    @Test
    public void testCreatePartitionedBucketedTableAs()
    {
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAs(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_as" + randomNameSuffix();

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
                getParallelWriteSession(getSession()),
                createTable,
                "SELECT count(*) FROM orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testCreatePartitionedBucketedTableWithNullsAs()
    {
        testCreatePartitionedBucketedTableWithNullsAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableWithNullsAs(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_partitioned_bucketed_table_with_nulls_as" + randomNameSuffix();

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
                getParallelWriteSession(getSession()),
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
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testUnpartitionedInsertWithMultipleFiles()
    {
        String tableName = "test_unpartitioned_insert_with_multiple_files" + randomNameSuffix();
        try {
            @Language("SQL") String createTargetTable = "" +
                    "CREATE TABLE " + tableName + " " +
                    "WITH (format = 'ORC') " +
                    "AS " +
                    "SELECT * " +
                    "FROM tpch.sf1.orders";
            assertUpdate(singleWriterWithTinyTargetFileSize(), createTargetTable, "SELECT 1500000");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private Session singleWriterWithTinyTargetFileSize()
    {
        return Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .setSystemProperty("task_scale_writers_enabled", "false")
                .setSystemProperty("query_max_memory_per_node", "100MB")
                .setCatalogSessionProperty(catalog, "target_max_file_size", "1B")
                .build();
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

            Session session = getParallelWriteSession(getSession());
            assertUpdate(session, createSourceTable, "SELECT count(*) FROM orders");
            assertUpdate(session, createTargetTable, "SELECT count(*) FROM orders");

            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getPlannerContext().getMetadata(), getQueryRunner().getAccessControl()).execute(
                    session,
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
        String tableName = "test_create_partitioned_bucketed_table_as_with_union_all" + randomNameSuffix();

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
                getParallelWriteSession(getSession()),
                createTable,
                "SELECT count(*) FROM orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    private void verifyPartitionedBucketedTable(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("orderstatus"));
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKETED_BY_PROPERTY, ImmutableList.of("custkey", "custkey2"));
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKET_COUNT_PROPERTY, 11);

        List<?> partitions = getPartitions(tableName);
        assertThat(partitions).hasSize(3);

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
        String tableName = "test_create_invalid_bucketed_table" + randomNameSuffix();

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

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
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
        String tableName = "test_insert_partitioned_bucketed_table_few_rows" + randomNameSuffix();

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
                getParallelWriteSession(session),
                "INSERT INTO " + tableName + " " +
                        "VALUES " +
                        "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                        "  ('aa', 'bb', 'cc'), " +
                        "  ('aaa', 'bbb', 'ccc')",
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertUpdate(session, "DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
    }

    private void verifyPartitionedBucketedTableAsFewRows(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("partition_key"));
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKETED_BY_PROPERTY, ImmutableList.of("bucket_key"));
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKET_COUNT_PROPERTY, 11);

        List<?> partitions = getPartitions(tableName);
        assertThat(partitions).hasSize(3);

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
        String tableName = "test_cast_null_to_column_types" + randomNameSuffix();

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
        String tableName = "test_insert_empty_partitioned_unbucketed_table" + randomNameSuffix();
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
        String tableName = "test_register_partition_for_table" + randomNameSuffix();
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
        assertThat(paths).hasSize(3);

        String firstPartition = Location.of((String) paths.get(0).getField(0)).parentDirectory().toString();

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
    public void testUnregisterPartitionWithNullArgument()
    {
        assertQueryFails("CALL system.unregister_partition(NULL, 'page_views', ARRAY['country'], ARRAY['US'])", ".*schema_name cannot be null.*");
        assertQueryFails("CALL system.unregister_partition('web', NULL, ARRAY['country'], ARRAY['US'])", ".*table_name cannot be null.*");
        assertQueryFails("CALL system.unregister_partition('web', 'page_views', NULL, ARRAY['US'])", ".*partition_columns cannot be null.*");
        assertQueryFails("CALL system.unregister_partition('web', 'page_views', ARRAY['country'], NULL)", ".*partition_values cannot be null.*");
    }

    @Test
    public void testRegisterPartitionWithNullArgument()
    {
        assertQueryFails("CALL system.register_partition(NULL, 'page_views', ARRAY['country'], ARRAY['US'])", ".*schema_name cannot be null.*");
        assertQueryFails("CALL system.register_partition('web', NULL, ARRAY['country'], ARRAY['US'])", ".*table_name cannot be null.*");
        assertQueryFails("CALL system.register_partition('web', 'page_views', NULL, ARRAY['US'])", ".*partition_columns cannot be null.*");
        assertQueryFails("CALL system.register_partition('web', 'page_views', ARRAY['country'], NULL)", ".*partition_values cannot be null.*");
    }

    @Test
    public void testCreateEmptyBucketedPartition()
    {
        testWithAllStorageFormats(this::testCreateEmptyBucketedPartition);
    }

    private void testCreateEmptyBucketedPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_empty_partitioned_bucketed_table" + randomNameSuffix();
        createPartitionedBucketedTable(session, tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String sql = format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", TPCH_SCHEMA, tableName, orderStatusList.get(i));
            assertUpdate(session, sql);
            assertQuery(
                    session,
                    format("SELECT count(*) FROM \"%s$partitions\"", tableName),
                    "SELECT " + (i + 1));

            assertQueryFails(session, sql, "Partition already exists.*");
        }

        assertUpdate(session, "DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
    }

    @Test
    public void testCreateEmptyPartitionOnNonExistingTable()
    {
        assertQueryFails(
                format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, "non_existing_table", "empty"),
                format("Table '%s.%s' does not exist", TPCH_SCHEMA, "non_existing_table"));
    }

    @Test
    public void testCreateEmptyPartitionWithNullArgument()
    {
        assertQueryFails("CALL system.create_empty_partition(NULL, 'page_views', ARRAY['country'], ARRAY['US'])", "schema_name cannot be null");
        assertQueryFails("CALL system.create_empty_partition('web', NULL, ARRAY['country'], ARRAY['US'])", "table_name cannot be null");
        assertQueryFails("CALL system.create_empty_partition('web', 'page_views', NULL, ARRAY['US'])", "partition_columns cannot be null");
        assertQueryFails("CALL system.create_empty_partition('web', 'page_views', ARRAY['country'], NULL)", "partition_values cannot be null");
    }

    @Test
    public void testInsertPartitionedBucketedTable()
    {
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_bucketed_table" + randomNameSuffix();
        createPartitionedBucketedTable(getSession(), tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(getSession()),
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
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    private void createPartitionedBucketedTable(Session session, String tableName, HiveStorageFormat storageFormat)
    {
        assertUpdate(
                session,
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
        String tableName = "test_insert_partitioned_bucketed_table_with_union_all" + randomNameSuffix();

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
                    getParallelWriteSession(getSession()),
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
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testInsertTwiceToSamePartitionedBucket()
    {
        String tableName = "test_insert_twice_to_same_partitioned_bucket" + randomNameSuffix();
        createPartitionedBucketedTable(getSession(), tableName, HiveStorageFormat.RCBINARY);

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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

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

        assertThat(getQueryRunner().tableExists(session, "test_insert_format_table")).isFalse();
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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("ship_priority", "order_status"));

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
        assertThat(partitions).hasSize(3);

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

        assertThat(getQueryRunner().tableExists(session, "test_insert_partitioned_table")).isFalse();
    }

    @Test
    public void testInsertPartitionedTableExistingPartition()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTableExistingPartition);
    }

    private void testInsertPartitionedTableExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_existing_partition" + randomNameSuffix();

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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("order_status"));

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
        assertThat(partitions).hasSize(3);

        assertQuery(
                session,
                "SELECT * FROM " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
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
        String tableName = "test_insert_partitioned_table_overwrite_existing_partition" + randomNameSuffix();

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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("order_status"));

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
            assertThat(partitions).hasSize(3);

            assertQuery(
                    session,
                    "SELECT * FROM " + tableName,
                    format("SELECT orderkey, comment, orderstatus FROM orders WHERE orderkey %% 3 = %d", i));
        }
        assertUpdate(session, "DROP TABLE " + tableName);

        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
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
        abort("Covered by testInsertUnicode");
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
        String tableName = "test_partition_per_scan_limit" + randomNameSuffix();
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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("part"));

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
        String tableName = "test_multi_partition_per_scan_limit" + randomNameSuffix();
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

        // verify can query less than 1000 partitions
        assertThat(query("SELECT * FROM " + tableName + " WHERE part1 % 400 = 3 AND part1 IS NOT NULL"))
                .matches("VALUES (VARCHAR 'bar', BIGINT '3', BIGINT '3')");

        // verify can query 1000 partitions
        assertThat(query("SELECT count(*) FROM " + tableName + " WHERE part1 IS NULL AND part2 < 1001"))
                .matches("VALUES BIGINT '1000'");

        // verify cannot query more than 1000 partitions
        assertThat(query("SELECT count(*) FROM " + tableName + " WHERE part1 IS NULL AND part2 <= 1001"))
                .failure().hasMessage("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName);
        assertThat(query("SELECT count(*) FROM " + tableName))
                .failure().hasMessage("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName);

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
        String tableName = "test_show_columns_from_partitions" + randomNameSuffix();

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
                ".*Table '.*\\.tpch\\.\"\\$partitions\"' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"orders$partitions\"",
                ".*Table '.*\\.tpch\\.\"orders\\$partitions\"' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"blah$partitions\"",
                ".*Table '.*\\.tpch\\.\"blah\\$partitions\"' does not exist");
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
                ".*Table '.*\\.tpch\\.\"test_partitions_invalid\\$partitions\\$partitions\"' does not exist");

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"non_existent$partitions\"",
                ".*Table '.*\\.tpch\\.\"non_existent\\$partitions\"' does not exist");
    }

    @Test
    public void testInsertUnpartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertUnpartitionedTable);
    }

    private void testInsertUnpartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_unpartitioned_table" + randomNameSuffix();

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
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

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

        assertThat(getQueryRunner().tableExists(session, tableName)).isFalse();
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned AS SELECT orderstatus FROM tpch.tiny.orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete_unpartitioned");

        MaterializedResult result = computeActual("SELECT * FROM test_delete_unpartitioned");
        assertThat(result.getRowCount()).isEqualTo(0);

        assertUpdate("DROP TABLE test_delete_unpartitioned");

        assertThat(getQueryRunner().tableExists(getSession(), "test_delete_unpartitioned")).isFalse();
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
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);

        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' AND linenumber<>3");

        assertUpdate("DROP TABLE test_metadata_delete");

        assertThat(getQueryRunner().tableExists(getSession(), "test_metadata_delete")).isFalse();
    }

    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), metadata, getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, schema, tableName));
                    assertThat(tableHandle).isPresent();
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }

    private Object getHiveTableProperty(String tableName, Function<HiveTableHandle, Object> propertyGetter)
    {
        Session session = getSession();
        Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), metadata, getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    QualifiedObjectName name = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    TableHandle table = metadata.getTableHandle(transactionSession, name)
                            .orElseThrow(() -> new AssertionError("table not found: " + name));
                    table = metadata.applyFilter(transactionSession, table, Constraint.alwaysTrue())
                            .orElseThrow(() -> new AssertionError("applyFilter did not return a result"))
                            .getHandle();
                    return propertyGetter.apply((HiveTableHandle) table.connectorHandle());
                });
    }

    private List<?> getPartitions(String tableName)
    {
        return (List<?>) getHiveTableProperty(tableName, handle -> handle.getPartitions().get());
    }

    private int getBucketCount(String tableName)
    {
        return (int) getHiveTableProperty(tableName, table -> table.getBucketHandle().get().tableBucketCount());
    }

    @Test
    public void testShowColumnsPartitionKey()
    {
        assertUpdate("" +
                "CREATE TABLE test_show_columns_partition_key\n" +
                "(grape bigint, orange bigint, pear varchar(65535), mango integer, lychee smallint, kiwi tinyint, apple varchar, pineapple varchar(65535))\n" +
                "WITH (partitioned_by = ARRAY['apple', 'pineapple'])");

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
        assertThat(query("SHOW COLUMNS FROM test_show_columns_partition_key")).result().matches(expected);
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

    @Test
    public void testTemporalArrays()
    {
        testTemporalArrays(HiveTimestampPrecision.MILLISECONDS);
        testTemporalArrays(HiveTimestampPrecision.MICROSECONDS);
        testTemporalArrays(HiveTimestampPrecision.NANOSECONDS);
    }

    private void testTemporalArrays(HiveTimestampPrecision timestampPrecision)
    {
        Session session = withTimestampPrecision(getSession(), timestampPrecision);
        assertUpdate("DROP TABLE IF EXISTS tmp_array11");
        assertUpdate("CREATE TABLE tmp_array11 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array11");
        assertUpdate("DROP TABLE IF EXISTS tmp_array12");
        assertUpdate("CREATE TABLE tmp_array12 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult(session, "SELECT col[1] FROM tmp_array12");
    }

    @Test
    public void testMaps()
    {
        testMaps(HiveTimestampPrecision.MILLISECONDS);
        testMaps(HiveTimestampPrecision.MICROSECONDS);
        testMaps(HiveTimestampPrecision.NANOSECONDS);
    }

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
        String tableName = "test_dereferences" + randomNameSuffix();
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
        String tableName = "test_dereferences_with_nulls" + randomNameSuffix();
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
        assertThat(ordersTableMetadata.metadata().getProperties()).containsEntry(BUCKETED_BY_PROPERTY, ImmutableList.of("custkey"));
        assertThat(ordersTableMetadata.metadata().getProperties()).containsEntry(BUCKET_COUNT_PROPERTY, 11);

        TableMetadata customerTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "customer");
        assertThat(customerTableMetadata.metadata().getProperties()).containsEntry(BUCKETED_BY_PROPERTY, ImmutableList.of("custkey"));
        assertThat(customerTableMetadata.metadata().getProperties()).containsEntry(BUCKET_COUNT_PROPERTY, 11);
    }

    @Test
    public void testBucketedExecution()
    {
        assertQuery(bucketedSession, "SELECT count(*) a FROM orders t1 JOIN orders t2 on t1.custkey=t2.custkey");
        assertQuery(bucketedSession, "SELECT count(*) a FROM orders t1 JOIN customer t2 on t1.custkey=t2.custkey", "SELECT count(*) FROM orders");
        assertQuery(bucketedSession, "SELECT count(distinct custkey) FROM orders");

        assertQuery("SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testSingleWriter()
    {
        try {
            // Small table that will only have one writer
            @Language("SQL") String createTableSql =
                    """
                    CREATE TABLE scale_writers_small
                    WITH (format = 'PARQUET')
                    AS SELECT * FROM tpch.tiny.orders\
                    """;
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("task_min_writer_count", "1")
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("task_scale_writers_enabled", "false")
                            .setSystemProperty("writer_scaling_min_data_processed", "100MB")
                            .build(),
                    createTableSql,
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());

            assertThat(computeActual("SELECT count(DISTINCT \"$path\") FROM scale_writers_small").getOnlyValue()).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_small");
        }
    }

    @Test
    public void testMultipleWriters()
    {
        try {
            // We need to use large table (sf2) to see the effect. Otherwise, a single writer will write the entire
            // data before ScaledWriterScheduler is able to scale it to multiple machines.
            @Language("SQL") String createTableSql = "CREATE TABLE scale_writers_large WITH (format = 'PARQUET') AS " +
                    "SELECT * FROM tpch.sf2.orders";
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("task_min_writer_count", "1")
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("task_scale_writers_enabled", "false")
                            .setSystemProperty("writer_scaling_min_data_processed", "1MB")
                            .build(),
                    createTableSql,
                    (long) computeActual("SELECT count(*) FROM tpch.sf2.orders").getOnlyValue());

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM scale_writers_large");
            long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
            assertThat(files).isBetween(2L, workers);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_large");
        }
    }

    @Test
    public void testMultipleWritersWithSkewedData()
    {
        try {
            // We need to use large table (sf1) to see the effect. Otherwise, a single writer will write the entire
            // data before ScaledWriterScheduler is able to scale it to multiple machines.
            // Skewed table that will scale writers to multiple machines.
            String selectSql =
                    """
                    SELECT t1.* FROM (SELECT *, case when orderkey >= 0 then 1 else orderkey end as join_key FROM tpch.sf1.orders) t1
                    INNER JOIN (SELECT orderkey FROM tpch.tiny.orders) t2
                    ON t1.join_key = t2.orderkey
                    """;
            @Language("SQL") String createTableSql = "CREATE TABLE scale_writers_skewed WITH (format = 'PARQUET') AS " + selectSql;
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("task_min_writer_count", "1")
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("task_scale_writers_enabled", "false")
                            .setSystemProperty("writer_scaling_min_data_processed", "0.1MB")
                            .setSystemProperty("join_distribution_type", "PARTITIONED")
                            .build(),
                    createTableSql,
                    (long) computeActual("SELECT count(*) FROM (" + selectSql + ")")
                            .getOnlyValue());

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM scale_writers_skewed");
            long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
            assertThat(files).isBetween(2L, workers);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS skewed_table");
            assertUpdate("DROP TABLE IF EXISTS scale_writers_skewed");
        }
    }

    @Test
    public void testMultipleWritersWhenTaskScaleWritersIsEnabled()
    {
        long taskMaxScaleWriterCount = 4;
        testTaskScaleWriters(getSession(), DataSize.of(200, KILOBYTE), (int) taskMaxScaleWriterCount, DataSize.of(64, GIGABYTE))
                .isBetween(2L, taskMaxScaleWriterCount);
    }

    @Test
    public void testTaskWritersDoesNotScaleWithLargeMinWriterSize()
    {
        // In the case of streaming, the number of writers is equal to the number of workers
        testTaskScaleWriters(getSession(), DataSize.of(2, GIGABYTE), 4, DataSize.of(64, GIGABYTE))
                .isEqualTo(1);
    }

    @Test
    public void testMultipleWritersWhenTaskScaleWritersIsEnabledWithMemoryLimit()
    {
        testTaskScaleWriters(getSession(), DataSize.of(200, KILOBYTE), 4, DataSize.of(256, MEGABYTE))
                // There shouldn't be no scaling as the memory limit is too low
                .isEqualTo(1L);
    }

    @Test
    public void testWriterTaskCountLimitUnpartitioned()
    {
        testLimitWriterTasks(2, 2, true, true, false, DataSize.of(1, MEGABYTE));
        testLimitWriterTasks(2, 2, false, true, false, DataSize.of(1, MEGABYTE));
        testLimitWriterTasks(2, 3, false, false, false, DataSize.of(1, MEGABYTE));
    }

    @Test
    public void testWriterTaskCountLimitPartitionedScaleWritersDisabled()
    {
        testLimitWriterTasks(2, 2, false, true, true, DataSize.of(1, MEGABYTE));
    }

    @Test
    public void testWriterTaskCountLimitPartitionedScaleWritersEnabled()
    {
        testLimitWriterTasks(2, 4, true, true, true, DataSize.of(1, MEGABYTE));
        // Since we track page size for scaling writer instead of actual compressed output file size, we need to have a
        // larger threshold for writerScalingMinDataProcessed. This way we can ensure that the writer scaling is not triggered.
        testLimitWriterTasks(2, 2, true, true, true, DataSize.of(128, MEGABYTE));
    }

    private void testLimitWriterTasks(int maxWriterTasks, int expectedFilesCount, boolean scaleWritersEnabled, boolean redistributeWrites, boolean partitioned, DataSize writerScalingMinDataProcessed)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SCALE_WRITERS, Boolean.toString(scaleWritersEnabled))
                .setSystemProperty(MAX_WRITER_TASK_COUNT, Integer.toString(maxWriterTasks))
                .setSystemProperty(REDISTRIBUTE_WRITES, Boolean.toString(redistributeWrites))
                .setSystemProperty(TASK_MIN_WRITER_COUNT, "1")
                .setSystemProperty(WRITER_SCALING_MIN_DATA_PROCESSED, writerScalingMinDataProcessed.toString())
                .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                .setSystemProperty(SKEWED_PARTITION_MIN_DATA_PROCESSED_REBALANCE_THRESHOLD, "10MB")
                .build();
        String tableName = "writing_tasks_limit_%s".formatted(randomNameSuffix());
        @Language("SQL") String createTableSql = format(
                "CREATE TABLE %s WITH (format = 'ORC' %s) AS SELECT *, mod(orderkey, 2) as part_key FROM tpch.sf2.orders LIMIT",
                tableName, partitioned ? ", partitioned_by = ARRAY['part_key']" : "");
        try {
            assertUpdate(session, createTableSql, (long) computeActual("SELECT count(*) FROM tpch.sf2.orders").getOnlyValue());
            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM %s".formatted(tableName));
            assertThat(files).isEqualTo(expectedFilesCount);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS %s".formatted(tableName));
        }
    }

    protected AbstractLongAssert<?> testTaskScaleWriters(
            Session session,
            DataSize writerScalingMinDataProcessed,
            int taskMaxScaleWriterCount,
            DataSize queryMaxMemory)
    {
        String tableName = "task_scale_writers_" + randomNameSuffix();
        try {
            @Language("SQL") String createTableSql = format(
                    "CREATE TABLE %s WITH (format = 'ORC') AS SELECT * FROM tpch.sf1.orders",
                    tableName);
            assertUpdate(
                    Session.builder(session)
                            .setSystemProperty(SCALE_WRITERS, "false")
                            .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                            .setSystemProperty(WRITER_SCALING_MIN_DATA_PROCESSED, writerScalingMinDataProcessed.toString())
                            .setSystemProperty(TASK_MAX_WRITER_COUNT, String.valueOf(taskMaxScaleWriterCount))
                            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, queryMaxMemory.toString())
                            .setSystemProperty(MAX_WRITER_TASK_COUNT, Integer.toString(1))
                            // Set the value higher than sf1 input data size such that fault-tolerant scheduler
                            // shouldn't add new task and scaling only happens through the local scaling exchange.
                            .setSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MIN, "2GB")
                            .setSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE_MAX, "2GB")
                            .setSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MIN, "2GB")
                            .setSystemProperty(FAULT_TOLERANT_EXECUTION_ARBITRARY_DISTRIBUTION_WRITE_TASK_TARGET_SIZE_MAX, "2GB")
                            .setSystemProperty(FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_COMPUTE_TASK_TARGET_SIZE, "2GB")
                            .setSystemProperty(FAULT_TOLERANT_EXECUTION_HASH_DISTRIBUTION_WRITE_TASK_TARGET_SIZE, "2GB")
                            .build(),
                    createTableSql,
                    1500000);

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName);
            return assertThat(files);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
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
        assertThat(getOnlyElement(actualResult.getOnlyColumnAsSet())).isEqualTo(createTableSql);

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
                        "   orc_bloom_filter_columns = ARRAY['c1','c 2'],\n" +
                        "   orc_bloom_filter_fpp = 7E-1,\n" +
                        "   partitioned_by = ARRAY['c5'],\n" +
                        "   sorted_by = ARRAY['c1','c 2 DESC']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "\"test_show_create_table'2\"");
        assertUpdate(createTableSql);
        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table'2\"");
        assertThat(getOnlyElement(actualResult.getOnlyColumnAsSet())).isEqualTo(createTableSql);

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
        assertThat(getOnlyElement(actualResult.getOnlyColumnAsSet())).isEqualTo(createTableSql);
    }

    @Test
    public void testShowCreateTableWithColumnProperties()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_show_create_table_with_column_properties",
                "(a INT, b INT WITH (partition_projection_type = 'INTEGER', partition_projection_range = ARRAY['0', '10'])) " +
                        "WITH (" +
                        "    partition_projection_enabled = true," +
                        "    partitioned_by = ARRAY['b']," +
                        "    partition_projection_location_template = 's3://example/${b}')")) {
            String result = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(result).isEqualTo("CREATE TABLE hive.tpch." + table.getName() + " (\n" +
                    "   a integer,\n" +
                    "   b integer WITH (partition_projection_range = ARRAY['0','10'], partition_projection_type = 'INTEGER')\n" +
                    ")\n" +
                    "WITH (\n" +
                    "   format = 'ORC',\n" +
                    "   partition_projection_enabled = true,\n" +
                    "   partition_projection_location_template = 's3://example/${b}',\n" +
                    "   partitioned_by = ARRAY['b']\n" +
                    ")");
        }
    }

    private void testCreateExternalTable(
            String tableName,
            String fileContents,
            String expectedResults,
            List<String> tableProperties)
            throws Exception
    {
        TrinoFileSystem fileSystem = getTrinoFileSystem();
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("text.text");
        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            out.write(fileContents.getBytes(UTF_8));
        }

        // Table properties
        StringJoiner propertiesSql = new StringJoiner(",\n   ");
        propertiesSql.add(format("external_location = '%s'", tempDir));
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
        assertThat(actual.getOnlyValue()).isEqualTo(createTableSql);

        assertQuery(format("SELECT col1, col2 from %s", tableName), expectedResults);
        assertUpdate(format("DROP TABLE %s", tableName));
        assertThat(fileSystem.newInputFile(dataFile).exists()).isTrue(); // file should still exist after drop
        fileSystem.deleteDirectory(tempDir);
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
        assertThat(actual.getOnlyValue()).isEqualTo(createTableSql);
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
        assertThat(actual.getOnlyValue()).isEqualTo(createTableSql);
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
        assertThat(actual.getOnlyValue()).isEqualTo(createTableSql);
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

        assertUpdate("DROP TABLE csv_table_skip_footer");

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
        assertThat(getQueryRunner().tableExists(getSession(), "test_path")).isTrue();

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_path");
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(STORAGE_FORMAT_PROPERTY, storageFormat);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.columns();
        assertThat(columnMetadatas).hasSize(columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertThat(columnMetadata.getName()).isEqualTo(columnNames.get(i));
            if (columnMetadata.getName().equals(PATH_COLUMN_NAME)) {
                // $path should be hidden column
                assertThat(columnMetadata.isHidden()).isTrue();
            }
        }
        assertThat(getPartitions("test_path")).hasSize(3);

        MaterializedResult results = computeActual(session, format("SELECT *, \"%s\" FROM test_path", PATH_COLUMN_NAME));
        Map<Integer, String> partitionPathMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            String pathName = (String) row.getField(2);
            String parentDirectory = Location.of(pathName).parentDirectory().toString();

            assertThat(pathName.length() > 0).isTrue();
            assertThat(col0 % 3).isEqualTo(col1);
            if (partitionPathMap.containsKey(col1)) {
                // the rows in the same partition should be in the same partition directory
                assertThat(partitionPathMap).containsEntry(col1, parentDirectory);
            }
            else {
                partitionPathMap.put(col1, parentDirectory);
            }
        }
        assertThat(partitionPathMap).hasSize(3);

        assertUpdate(session, "DROP TABLE test_path");
        assertThat(getQueryRunner().tableExists(session, "test_path")).isFalse();
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
        assertThat(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column")).isTrue();

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_bucket_hidden_column");
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKETED_BY_PROPERTY, ImmutableList.of("col0"));
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(BUCKET_COUNT_PROPERTY, 2);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, BUCKET_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.columns();
        assertThat(columnMetadatas).hasSize(columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertThat(columnMetadata.getName()).isEqualTo(columnNames.get(i));
            if (columnMetadata.getName().equals(BUCKET_COLUMN_NAME)) {
                // $bucket_number should be hidden column
                assertThat(columnMetadata.isHidden()).isTrue();
            }
        }
        assertThat(getBucketCount("test_bucket_hidden_column")).isEqualTo(2);

        MaterializedResult results = computeActual(format("SELECT *, \"%1$s\" FROM test_bucket_hidden_column WHERE \"%1$s\" = 1",
                BUCKET_COLUMN_NAME));
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            int bucket = (int) row.getField(2);

            assertThat(col1).isEqualTo(col0 + 11);
            assertThat(col1 % 2).isZero();

            // Because Hive's hash function for integer n is h(n) = n.
            assertThat(bucket).isEqualTo(col0 % 2);
        }
        assertThat(results.getRowCount()).isEqualTo(4);

        assertUpdate("DROP TABLE test_bucket_hidden_column");
        assertThat(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column")).isFalse();
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
        assertThat(getQueryRunner().tableExists(getSession(), "test_file_size")).isTrue();

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_size");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.columns();
        assertThat(columnMetadatas).hasSize(columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertThat(columnMetadata.getName()).isEqualTo(columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_SIZE_COLUMN_NAME)) {
                assertThat(columnMetadata.isHidden()).isTrue();
            }
        }
        assertThat(getPartitions("test_file_size")).hasSize(3);

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_file_size", FILE_SIZE_COLUMN_NAME));
        Map<Integer, Long> fileSizeMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            long fileSize = (Long) row.getField(2);

            assertThat(fileSize > 0).isTrue();
            assertThat(col0 % 3).isEqualTo(col1);
            if (fileSizeMap.containsKey(col1)) {
                assertThat(fileSizeMap.get(col1).longValue()).isEqualTo(fileSize);
            }
            else {
                fileSizeMap.put(col1, fileSize);
            }
        }
        assertThat(fileSizeMap).hasSize(3);

        assertUpdate("DROP TABLE test_file_size");
    }

    @Test
    public void testFileModifiedTimeHiddenColumn()
    {
        testFileModifiedTimeHiddenColumn(HiveTimestampPrecision.MILLISECONDS);
        testFileModifiedTimeHiddenColumn(HiveTimestampPrecision.MICROSECONDS);
        testFileModifiedTimeHiddenColumn(HiveTimestampPrecision.NANOSECONDS);
    }

    private void testFileModifiedTimeHiddenColumn(HiveTimestampPrecision precision)
    {
        @Language("SQL") String createTable = "CREATE TABLE test_file_modified_time " +
                "WITH (" +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        long beforeCreateSecond = Instant.now().getEpochSecond();
        assertUpdate(createTable, 8);
        long afterCreateSecond = Instant.now().getEpochSecond() + 1; // +1 to round up, not truncate
        assertThat(getQueryRunner().tableExists(getSession(), "test_file_modified_time")).isTrue();

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_modified_time");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.columns();
        assertThat(columnMetadatas).hasSize(columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertThat(columnMetadata.getName()).isEqualTo(columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                assertThat(columnMetadata.isHidden()).isTrue();
            }
        }
        assertThat(getPartitions("test_file_modified_time")).hasSize(3);

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
            assertThat(fileModifiedTime.getEpochSecond())
                    .isBetween(beforeCreateSecond, afterCreateSecond);
            assertThat(col0 % 3).isEqualTo(col1);
            if (fileModifiedTimeMap.containsKey(col1)) {
                assertThat(fileModifiedTimeMap).containsEntry(col1, fileModifiedTime);
            }
            else {
                fileModifiedTimeMap.put(col1, fileModifiedTime);
            }
        }
        assertThat(fileModifiedTimeMap).hasSize(3);

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
        assertThat(getQueryRunner().tableExists(getSession(), "test_partition_hidden_column")).isTrue();

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partition_hidden_column");
        assertThat(tableMetadata.metadata().getProperties()).containsEntry(PARTITIONED_BY_PROPERTY, ImmutableList.of("col1", "col2"));

        List<String> columnNames = ImmutableList.of("col0", "col1", "col2", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, PARTITION_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.columns();
        assertThat(columnMetadatas).hasSize(columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertThat(columnMetadata.getName()).isEqualTo(columnNames.get(i));
            if (columnMetadata.getName().equals(PARTITION_COLUMN_NAME)) {
                assertThat(columnMetadata.isHidden()).isTrue();
            }
        }
        assertThat(getPartitions("test_partition_hidden_column")).hasSize(9);

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_partition_hidden_column", PARTITION_COLUMN_NAME));
        for (MaterializedRow row : results.getMaterializedRows()) {
            String actualPartition = (String) row.getField(3);
            String expectedPartition = format("col1=%s/col2=%s", row.getField(1), row.getField(2));
            assertThat(actualPartition).isEqualTo(expectedPartition);
        }
        assertThat(results.getRowCount()).isEqualTo(9);

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
            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getPlannerContext().getMetadata(), getQueryRunner().getAccessControl())
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

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getPlannerContext().getMetadata(), getQueryRunner().getAccessControl())
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

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getPlannerContext().getMetadata(), getQueryRunner().getAccessControl())
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
    @Override
    public void testDropAndAddColumnWithSameName()
    {
        // Override because Hive connector can access old data after dropping and adding a column with same name
        assertThatThrownBy(super::testDropAndAddColumnWithSameName)
                .hasMessageContaining(
                        """
                        Actual rows (up to 100 of 1 extra rows shown, 1 rows in total):
                            [1, 2]\
                            """);
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
    public void testAvroTimestampUpCasting()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_avro_timestamp_upcasting WITH (format = 'AVRO') AS SELECT TIMESTAMP '1994-09-27 11:23:45.678' my_timestamp";

        //avro only stores as millis
        assertUpdate(createTable, 1);

        // access with multiple precisions
        assertQuery(withTimestampPrecision(getSession(), HiveTimestampPrecision.MILLISECONDS),
                "SELECT * from test_avro_timestamp_upcasting",
                "VALUES (TIMESTAMP '1994-09-27 11:23:45.678')");

        // access with multiple precisions
        assertQuery(withTimestampPrecision(getSession(), HiveTimestampPrecision.MICROSECONDS),
                "SELECT * from test_avro_timestamp_upcasting",
                "VALUES (TIMESTAMP '1994-09-27 11:23:45.678000')");

        // access with multiple precisions
        assertQuery(withTimestampPrecision(getSession(), HiveTimestampPrecision.NANOSECONDS),
                "SELECT * from test_avro_timestamp_upcasting",
                "VALUES (TIMESTAMP '1994-09-27 11:23:45.678000000')");
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

        assertThat(actual.getTypes()).isEqualTo(expected.getTypes());
        assertThat(actual).containsExactlyElementsOf(expected);
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
                    "VALUES " +
                            "('aaa                 ', true), " +
                            "('bbb                 ', true), " +
                            "('bbc                 ', true), " +
                            "('bbd                 ', false)");

            assertQuery(
                    "SELECT a FROM test_table_with_char WHERE a <= 'bbc'",
                    "VALUES " +
                            "'aaa                 ', " +
                            "'bbb                 ', " +
                            "'bbc                 '");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char");
        }
    }

    @Test
    public void testMismatchedBucketWithBucketPredicate()
    {
        assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_with_bucket_predicate8");
        assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_with_bucket_predicate32");

        assertUpdate(
                "CREATE TABLE test_mismatch_bucketing_with_bucket_predicate8 " +
                        "WITH (bucket_count = 8, bucketed_by = ARRAY['key8']) AS " +
                        "SELECT nationkey key8, comment value8 FROM nation",
                25);
        assertUpdate(
                "CREATE TABLE test_mismatch_bucketing_with_bucket_predicate32 " +
                        "WITH (bucket_count = 32, bucketed_by = ARRAY['key32']) AS " +
                        "SELECT nationkey key32, comment value32 FROM nation",
                25);

        Session withMismatchOptimization = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "true")
                .build();
        Session withoutMismatchOptimization = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "false")
                .build();

        @Language("SQL") String query = "SELECT count(*) AS count " +
                "FROM (" +
                "  SELECT key32" +
                "  FROM test_mismatch_bucketing_with_bucket_predicate32" +
                "  WHERE \"$bucket\" between 16 AND 31" +
                ") a " +
                "JOIN test_mismatch_bucketing_with_bucket_predicate8 b " +
                "ON a.key32 = b.key8";

        assertQuery(withMismatchOptimization, query, "SELECT 9");
        assertQuery(withoutMismatchOptimization, query, "SELECT 9");

        assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_with_bucket_predicate8");
        assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_with_bucket_predicate32");
    }

    @Test
    public void testParquetTimestampPredicatePushdown()
    {
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123456"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123000000"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123000001"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123456789"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123456"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123000000"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123000001"));
        testParquetTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123456789"));
    }

    private void testParquetTimestampPredicatePushdown(HiveTimestampPrecision timestampPrecision, LocalDateTime value)
    {
        Session session = withTimestampPrecision(getSession(), timestampPrecision);
        String tableName = "test_parquet_timestamp_predicate_pushdown_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (t TIMESTAMP) WITH (format = 'PARQUET')");
        assertUpdate(session, format("INSERT INTO %s VALUES (%s)", tableName, formatTimestamp(value)), 1);
        assertQuery(session, "SELECT * FROM " + tableName, format("VALUES (%s)", formatTimestamp(value)));

        QueryRunner queryRunner = getQueryRunner();
        MaterializedResultWithPlan queryResult = queryRunner.executeWithPlan(
                session,
                format("SELECT * FROM %s WHERE t < %s", tableName, formatTimestamp(value)));
        assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes()).isEqualTo(0);

        queryResult = queryRunner.executeWithPlan(
                session,
                format("SELECT * FROM %s WHERE t > %s", tableName, formatTimestamp(value)));
        assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes()).isEqualTo(0);

        assertQueryStats(
                session,
                format("SELECT * FROM %s WHERE t = %s", tableName, formatTimestamp(value)),
                queryStats -> {
                    assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0);
                },
                results -> { });
    }

    @Test
    public void testOrcTimestampPredicatePushdown()
    {
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123456"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123000000"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123000001"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2012-10-31T01:00:08.123456789"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123456"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123000000"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123000001"));
        testOrcTimestampPredicatePushdown(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("1965-10-31T01:00:08.123456789"));
    }

    private void testOrcTimestampPredicatePushdown(HiveTimestampPrecision timestampPrecision, LocalDateTime value)
    {
        Session session = withTimestampPrecision(getSession(), timestampPrecision);
        assertUpdate("DROP TABLE IF EXISTS test_orc_timestamp_predicate_pushdown");
        assertUpdate("CREATE TABLE test_orc_timestamp_predicate_pushdown (t TIMESTAMP) WITH (format = 'ORC')");
        assertUpdate(session, format("INSERT INTO test_orc_timestamp_predicate_pushdown VALUES (%s)", formatTimestamp(value)), 1);
        assertQuery(session, "SELECT * FROM test_orc_timestamp_predicate_pushdown", format("VALUES (%s)", formatTimestamp(value)));

        // to account for the fact that ORC stats are stored at millisecond precision and Trino rounds timestamps,
        // we filter by timestamps that differ from the actual value by at least 1ms, to observe pruning
        QueryRunner queryRunner = getDistributedQueryRunner();
        MaterializedResultWithPlan queryResult = queryRunner.executeWithPlan(
                session,
                format("SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t < %s", formatTimestamp(value.minusNanos(MILLISECONDS.toNanos(1)))));
        assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes()).isEqualTo(0);

        queryResult = queryRunner.executeWithPlan(
                session,
                format("SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t > %s", formatTimestamp(value.plusNanos(MILLISECONDS.toNanos(1)))));
        assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes()).isEqualTo(0);

        assertQuery(session, "SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t < " + formatTimestamp(value.plusNanos(1)), format("VALUES (%s)", formatTimestamp(value)));

        assertQueryStats(
                session,
                format("SELECT * FROM test_orc_timestamp_predicate_pushdown WHERE t = %s", formatTimestamp(value)),
                queryStats -> {
                    assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0);
                },
                results -> { });
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
        String tableName = "test_parquet_dictionary_pushdown_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (n BIGINT) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1, 1, 2, 2, 4, 4, 5, 5", 8);
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE n = 3");
    }

    private QueryInfo getQueryInfo(QueryRunner queryRunner, MaterializedResultWithPlan queryResult)
    {
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryResult.queryId());
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
    public void testSchemaMismatchesWithDereferenceProjections()
    {
        testWithAllStorageFormats(this::testSchemaMismatchesWithDereferenceProjections);
    }

    private void testSchemaMismatchesWithDereferenceProjections(Session session, HiveStorageFormat format)
    {
        // Verify reordering of subfields between a partition column and a table column is not supported
        // eg. table column: a row(c varchar, b bigint), partition column: a row(b bigint, c varchar)
        String tableName = "evolve_test_" + randomNameSuffix();
        try {
            assertUpdate(session, "CREATE TABLE " + tableName + " (dummy bigint, a row(b bigint, c varchar), d bigint) with (format = '" + format + "', partitioned_by=array['d'])");
            assertUpdate(session, "INSERT INTO " + tableName + " values (10, row(1, 'abc'), 1)", 1);
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP COLUMN a");
            assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN a row(c varchar, b bigint)");
            assertUpdate(session, "INSERT INTO " + tableName + " values (20, row('def', 2), 2)", 1);
            assertQueryFails(session, "SELECT a.b FROM " + tableName + " where d = 1", ".*There is a mismatch between the table and partition schemas.*");
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS " + tableName);
        }

        // Subfield absent in partition schema is reported as null
        // i.e. "a.c" produces null for rows that were inserted before type of "a" was changed
        try {
            assertUpdate(session, "CREATE TABLE " + tableName + " (dummy bigint, a row(b bigint), d bigint) with (format = '" + format + "', partitioned_by=array['d'])");
            assertUpdate(session, "INSERT INTO " + tableName + " values (10, row(1), 1)", 1);
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP COLUMN a");
            assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN a row(b bigint, c varchar)");
            assertUpdate(session, "INSERT INTO " + tableName + " values (20, row(2, 'def'), 2)", 1);
            assertQuery(session, "SELECT a.c FROM " + tableName, "SELECT 'def' UNION SELECT null");
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS " + tableName);
        }

        // Verify field access when the row evolves without changes to field type
        try {
            assertUpdate(session, "CREATE TABLE " + tableName + " (dummy bigint, a row(b bigint, c varchar), d bigint) with (format = '" + format + "', partitioned_by=array['d'])");
            assertUpdate(session, "INSERT INTO " + tableName + " values (10, row(1, 'abc'), 1)", 1);
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP COLUMN a");
            assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN a row(b bigint, c varchar, e int)");
            assertUpdate(session, "INSERT INTO " + tableName + " values (20, row(2, 'def', 2), 2)", 1);
            assertQuery(session, "SELECT a.b FROM " + tableName, "VALUES 1, 2");
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testReadWithPartitionSchemaMismatch()
    {
        testWithAllStorageFormats(this::testReadWithPartitionSchemaMismatch);

        // test ORC in non-default configuration with by-name column mapping
        Session orcUsingColumnNames = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "orc_use_column_names", "true")
                .build();
        testWithStorageFormat(new TestingHiveStorageFormat(orcUsingColumnNames, ORC), this::testReadWithPartitionSchemaMismatchByName);

        // test PARQUET in non-default configuration with by-index column mapping
        Session parquetUsingColumnIndex = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "parquet_use_column_names", "false")
                .build();
        testWithStorageFormat(new TestingHiveStorageFormat(parquetUsingColumnIndex, PARQUET), this::testReadWithPartitionSchemaMismatchByIndex);
    }

    private void testReadWithPartitionSchemaMismatch(Session session, HiveStorageFormat format)
    {
        if (isMappingByName(format)) {
            testReadWithPartitionSchemaMismatchByName(session, format);
        }
        else {
            testReadWithPartitionSchemaMismatchByIndex(session, format);
        }
    }

    private boolean isMappingByName(HiveStorageFormat format)
    {
        return switch (format) {
            case PARQUET -> true;
            case AVRO -> true;
            case JSON -> true;
            case ORC -> false;
            case RCBINARY -> false;
            case RCTEXT -> false;
            case SEQUENCEFILE -> false;
            case OPENX_JSON -> false;
            case TEXTFILE -> false;
            case CSV -> false;
            case REGEX -> false;
        };
    }

    private void testReadWithPartitionSchemaMismatchByName(Session session, HiveStorageFormat format)
    {
        String tableName = testReadWithPartitionSchemaMismatchAddedColumns(session, format);

        // with mapping by name also test behavior with dropping columns
        // start with table with a, b, c, _part
        // drop b
        assertUpdate(session, "ALTER TABLE " + tableName + " DROP COLUMN b");
        // create new partition
        assertUpdate(session, "INSERT INTO " + tableName + " values (21, 22, 20)", 1); // a, c, _part
        assertQuery(session, "SELECT a, c, _part FROM " + tableName, "VALUES (1, null, 0), (11, 13, 10), (21, 22, 20)");
        assertQuery(session, "SELECT a, _part FROM " + tableName, "VALUES (1,  0), (11, 10), (21, 20)");
        // add d
        assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN d bigint");
        // create new partition
        assertUpdate(session, "INSERT INTO " + tableName + " values (31, 32, 33, 30)", 1); // a, c, d, _part
        assertQuery(session, "SELECT a, c, d, _part FROM " + tableName, "VALUES (1, null, null, 0), (11, 13, null, 10), (21, 22, null, 20), (31, 32, 33, 30)");
        assertQuery(session, "SELECT a, d, _part FROM " + tableName, "VALUES (1, null, 0), (11, null, 10), (21, null, 20), (31, 33, 30)");
    }

    private void testReadWithPartitionSchemaMismatchByIndex(Session session, HiveStorageFormat format)
    {
        // we are not dropping columns for format which use index based mapping as logic is confusing and not consistent between different formats
        testReadWithPartitionSchemaMismatchAddedColumns(session, format);
    }

    private String testReadWithPartitionSchemaMismatchAddedColumns(Session session, HiveStorageFormat format)
    {
        String tableName = "read_with_partition_schema_mismatch_by_name_" + randomNameSuffix();
        // create table with a, b, _part
        assertUpdate(session, "CREATE TABLE " + tableName + " (a bigint, b bigint, _part bigint) with (format = '" + format + "', partitioned_by=array['_part'])");
        // create new partition
        assertUpdate(session, "INSERT INTO " + tableName + " values (1, 2, 0)", 1); // a, b, _part
        assertQuery(session, "SELECT a, b, _part FROM " + tableName, "VALUES (1, 2, 0)");
        assertQuery(session, "SELECT a, _part FROM " + tableName, "VALUES (1, 0)");
        // add column c
        assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN c bigint");
        // create new partition
        assertUpdate(session, "INSERT INTO " + tableName + " values (11, 12, 13, 10)", 1); // a, b, c, _part
        assertQuery(session, "SELECT a, b, c, _part FROM " + tableName, "VALUES (1, 2, null, 0), (11, 12, 13, 10)");
        assertQuery(session, "SELECT a, c, _part FROM " + tableName, "VALUES (1, null, 0), (11, 13, 10)");
        assertQuery(session, "SELECT c, _part FROM " + tableName + " WHERE a < 7", "VALUES (null, 0)"); // common column used in WHERE but not in FROM
        assertQuery(session, "SELECT a, _part FROM " + tableName + " WHERE c > 7", "VALUES (11, 10)"); // missing column used in WHERE but not in FROM
        return tableName;
    }

    @Test
    public void testSubfieldReordering()
    {
        // Validate for formats for which subfield access is name based
        List<HiveStorageFormat> formats = ImmutableList.of(HiveStorageFormat.ORC, HiveStorageFormat.PARQUET, HiveStorageFormat.AVRO);
        String tableName = "evolve_test_" + randomNameSuffix();

        for (HiveStorageFormat format : formats) {
            // Subfields reordered in the file are read correctly. e.g. if partition column type is row(b bigint, c varchar) but the file
            // column type is row(c varchar, b bigint), "a.b" should read the correct field from the file.
            try {
                assertUpdate("CREATE TABLE " + tableName + " (dummy bigint, a row(b bigint, c varchar)) with (format = '" + format + "')");
                assertUpdate("INSERT INTO " + tableName + " values (1, row(1, 'abc'))", 1);
                assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN a");
                assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a row(c varchar, b bigint)");
                assertQuery("SELECT a.b FROM " + tableName, "VALUES 1");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS " + tableName);
            }

            // Assert that reordered subfields are read correctly for a two-level nesting. This is useful for asserting correct adaptation
            // of residue projections in HivePageSourceProvider
            try {
                assertUpdate("CREATE TABLE " + tableName + " (dummy bigint, a row(b bigint, c row(x bigint, y varchar))) with (format = '" + format + "')");
                assertUpdate("INSERT INTO " + tableName + " values (1, row(1, row(3, 'abc')))", 1);
                assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN a");
                assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a row(c row(y varchar, x bigint), b bigint)");
                // TODO: replace the following assertion with assertQuery once h2QueryRunner starts supporting row types
                assertQuerySucceeds("SELECT a.c.y, a.c FROM " + tableName);
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS " + tableName);
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

        String tableName = "test_parquet_by_column_index" + randomNameSuffix();

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

        String tableLocation = getTableLocation(tableName);

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

        String tableLocation = getTableLocation(singleColumnTableName);
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

        String tableLocation = getTableLocation(missingNestedFieldsTableName);
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
    public void testParquetIgnoreStatistics()
    {
        for (boolean ignoreStatistics : ImmutableList.of(true, false)) {
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty(catalog, "parquet_ignore_statistics", String.valueOf(ignoreStatistics))
                    .build();

            String tableName = "test_parquet_ignore_statistics" + randomNameSuffix();

            assertUpdate(session, format(
                    "CREATE TABLE %s(" +
                            "  a varchar, " +
                            "  b varchar) " +
                            "WITH (format='PARQUET', partitioned_by=ARRAY['b'])",
                    tableName));
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);

            assertQuery(
                    session,
                    "SELECT a, b FROM " + tableName,
                    "VALUES ('a', 'b')");

            assertQuery(
                    session,
                    "SELECT a, b FROM " + tableName + " WHERE b = 'b'",
                    "VALUES ('a', 'b')");

            assertQuery(
                    session,
                    "SELECT a, b FROM " + tableName + " WHERE a = 'a'",
                    "VALUES ('a', 'b')");

            assertUpdate(session, "DROP TABLE " + tableName);
        }
    }

    @Test
    public void testNestedColumnWithDuplicateName()
    {
        String tableName = "test_nested_column_with_duplicate_name" + randomNameSuffix();

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

        assertThat(computeActual("SELECT foo FROM " + tableName + " WHERE foo = 'a' AND root.foo = 'a'").getMaterializedRows()).isEmpty();
        assertThat(computeActual("SELECT foo FROM " + tableName + " WHERE foo = 'b' AND root.foo = 'b'").getMaterializedRows()).isEmpty();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testParquetNaNStatistics()
    {
        String tableName = "test_parquet_nan_statistics" + randomNameSuffix();

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

            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_bucketed_select");
        }
    }

    @Test
    public void testGroupedExecution() // TODO change the name
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
                    "CREATE TABLE test_grouped_joinN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);

            Session session = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "false")
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

            assertQuery(session, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(session, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(session, rightJoinBucketedTable, expectedOuterJoinQuery);

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
            assertQuery(session, crossJoin, expectedCrossJoinQuery);

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
            assertQuery(session, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery);

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

            assertQuery(session, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(session, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult);

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

            assertQuery(session, noSplits, expectedNoSplits);
            assertQuery(session, joinMismatchedBuckets, expectedJoinMismatchedBuckets);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join1");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join2");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join3");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinN");
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
                throw new AssertionError(format(
                        "Expected [\n%s\n] remote exchanges but found [\n%s\n] remote exchanges. Actual plan is [\n\n%s\n]",
                        expectedRemoteExchangesCount,
                        actualRemoteExchangesCount,
                        formatPlan(session, plan)));
            }
        };
    }

    private Consumer<Plan> assertLocalRepartitionedExchangesCount(int expectedLocalExchangesCount)
    {
        return plan -> {
            int actualLocalExchangesCount = searchFrom(plan.getRoot())
                    .where(node -> {
                        if (!(node instanceof ExchangeNode exchangeNode)) {
                            return false;
                        }

                        return exchangeNode.getScope() == ExchangeNode.Scope.LOCAL && exchangeNode.getType() == ExchangeNode.Type.REPARTITION;
                    })
                    .findAll()
                    .size();
            if (actualLocalExchangesCount != expectedLocalExchangesCount) {
                throw new AssertionError(format(
                        "Expected [\n%s\n] local repartitioned exchanges but found [\n%s\n] local repartitioned exchanges. Actual plan is [\n\n%s\n]",
                        expectedLocalExchangesCount,
                        actualLocalExchangesCount,
                        formatPlan(getSession(), plan)));
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
                    "VALUES ('khaki  ')");
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
        String tableName = "test_show_column_table" + randomNameSuffix();

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
        assertThat(computeActual(getColumnsSql).getOnlyColumnAsSet()).isEqualTo(ImmutableSet.of("a", "b", "c"));

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
        String tableName = "test_collect_column_statistics_on_create_table" + randomNameSuffix();
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

        String tableName = "test_stats_on_create_timestamp_with_precision" + randomNameSuffix();

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
        String tableName = "test_collect_column_statistics_on_insert" + randomNameSuffix();
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
        String tableName = "test_collect_column_statistics_empty_table" + randomNameSuffix();

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
        String tableName = "test_collect_column_statistics_partially_analyzed_table" + randomNameSuffix();

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
        String tableName = "test_analyze_empty_table" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (c_bigint BIGINT, c_varchar VARCHAR(2))", tableName));
        assertUpdate("ANALYZE " + tableName, 0);
    }

    @Test
    public void testInvalidAnalyzePartitionedTable()
    {
        String tableName = "test_invalid_analyze_partitioned_table" + randomNameSuffix();

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, format(".*Table 'hive.tpch.%s' does not exist.*", tableName));

        createPartitionedTableForAnalyzeTest(tableName);

        // Test invalid property
        assertQueryFails(format("ANALYZE %s WITH (error = 1)", tableName), "line 1:64: Catalog 'hive' analyze property 'error' does not exist");
        assertQueryFails(format("ANALYZE %s WITH (partitions = 1)", tableName), "\\Qline 1:64: Invalid value for catalog 'hive' analyze property 'partitions': Cannot convert [1] to array(array(varchar))\\E");
        assertQueryFails(format("ANALYZE %s WITH (partitions = NULL)", tableName), "\\Qline 1:64: Invalid null value for catalog 'hive' analyze property 'partitions' from [null]\\E");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[NULL])", tableName), ".*Invalid null value in analyze partitions property.*");

        // Test non-existed partition
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10']])", tableName), ".*Partition.*not found.*");

        // Test partition schema mismatch
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4']])", tableName), "Partition value count does not match partition column count");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10', 'error']])", tableName), "Partition value count does not match partition column count");

        // Drop the partitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInvalidAnalyzeUnpartitionedTable()
    {
        String tableName = "test_invalid_analyze_unpartitioned_table" + randomNameSuffix();

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
        String tableName = "test_analyze_partitioned_table" + randomNameSuffix();
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 24.0, 3.0, 0.25, null, null, null),
                    ('p_bigint', null, 2.0, 0.25, null, '7', '8'),
                    (null, null, null, null, 16.0, null, null)
                """);

        // No column stats after running an empty analyze
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), 0);
        assertQuery("SHOW STATS FOR " + tableName,
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 24.0, 3.0, 0.25, null, null, null),
                    ('p_bigint', null, 2.0, 0.25, null, '7', '8'),
                    (null, null, null, null, 16.0, null, null)
                """);

        // Run analyze on 3 partitions including a null partition and a duplicate partition
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]])", tableName), 12);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '0', '1'),
                    ('c_double', null, 2.0, 0.5, null, '1.2', '2.2'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '1', '2'),
                    ('c_double', null, 2.0, 0.5, null, '2.3', '3.3'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 1.0, 0.0, null, null, null),
                    ('c_bigint', null, 4.0, 0.0, null, '4', '7'),
                    ('c_double', null, 4.0, 0.0, null, '4.7', '7.7'),
                    ('c_timestamp', null, 4.0, 0.0, null, null, null),
                    ('c_varchar', 16.0, 4.0, 0.0, null, null, null),
                    ('c_varbinary', 8.0, null, 0.0, null, null, null),
                    ('p_varchar', 0.0, 0.0, 1.0, null, null, null),
                    ('p_bigint', 0.0, 0.0, 1.0, null, null, null),
                    (null, null, null, null, 4.0, null, null)
                """);

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '8', '8'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);

        // Run analyze on the whole table
        assertUpdate("ANALYZE\n" + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '0', '1'),
                    ('c_double', null, 2.0, 0.5, null, '1.2', '2.2'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '1', '2'),
                    ('c_double', null, 2.0, 0.5, null, '2.3', '3.3'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 1.0, 0.0, null, null, null),
                    ('c_bigint', null, 4.0, 0.0, null, '4', '7'),
                    ('c_double', null, 4.0, 0.0, null, '4.7', '7.7'),
                    ('c_timestamp', null, 4.0, 0.0, null, null, null),
                    ('c_varchar', 16.0, 4.0, 0.0, null, null, null),
                    ('c_varbinary', 8.0, null, 0.0, null, null, null),
                    ('p_varchar', 0.0, 0.0, 1.0, null, null, null),
                    ('p_bigint', 0.0, 0.0, 1.0, null, null, null),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '2', '3'),
                    ('c_double', null, 2.0, 0.5, null, '3.4', '4.4'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '8', '8'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);

        // Drop the partitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzePartitionedTableWithColumnSubset()
    {
        String tableName = "test_analyze_columns_partitioned_table" + randomNameSuffix();
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery(
                "SHOW STATS FOR\n" + tableName,
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 24.0, 3.0, 0.25, null, null, null),
                    ('p_bigint', null, 2.0, 0.25, null, '7', '8'),
                    (null, null, null, null, 16.0, null, null)
                """);

        // Run analyze on 3 partitions including a null partition and a duplicate partition,
        // restricting to just 2 columns (one duplicate)
        assertUpdate(
                format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]],\n" +
                        "columns = ARRAY['c_timestamp', 'c_varchar', 'c_timestamp'])", tableName), 12);

        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, 4.0, 0.0, null, null, null),
                    ('c_varchar', 16.0, 4.0, 0.0, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 0.0, 0.0, 1.0, null, null, null),
                    ('p_bigint', 0.0, 0.0, 1.0, null, null, null),
                    (null, null, null, null, 4.0, null, null)
                """);

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '8', '8'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);

        // Run analyze again, this time on 2 new columns (for all partitions); the previously computed stats
        // should be preserved
        assertUpdate(
                format("ANALYZE %s WITH (columns = ARRAY['c_bigint', 'c_double'])", tableName), 16);

        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '0', '1'),
                    ('c_double', null, 2.0, 0.5, null, '1.2', '2.2'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '1', '2'),
                    ('c_double', null, 2.0, 0.5, null, '2.3', '3.3'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, 4.0, 0.0, null, '4', '7'),
                    ('c_double', null, 4.0, 0.0, null, '4.7', '7.7'),
                    ('c_timestamp', null, 4.0, 0.0, null, null, null),
                    ('c_varchar', 16.0, 4.0, 0.0, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 0.0, 0.0, 1.0, null, null, null),
                    ('p_bigint', 0.0, 0.0, 1.0, null, null, null),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '2', '3'),
                    ('c_double', null, 2.0, 0.5, null, '3.4', '4.4'),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '8', '8'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(
                format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeUnpartitionedTable()
    {
        String tableName = "test_analyze_unpartitioned_table" + randomNameSuffix();
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, null, null, null, null, null),
                    ('p_bigint', null, null, null, null, null, null),
                    (null, null, null, null, 16.0, null, null)
                """);

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.375, null, null, null),
                    ('c_bigint', null, 8.0, 0.375, null, '0', '7'),
                    ('c_double', null, 10.0, 0.375, null, '1.2', '7.7'),
                    ('c_timestamp', null, 10.0, 0.375, null, null, null),
                    ('c_varchar', 40.0, 10.0, 0.375, null, null, null),
                    ('c_varbinary', 20.0, null, 0.375, null, null, null),
                    ('p_varchar', 24.0, 3.0, 0.25, null, null, null),
                    ('p_bigint', null, 2.0, 0.25, null, '7', '8'),
                    (null, null, null, null, 16.0, null, null)
                """);

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

        String tableName = "test_analyze_timestamp_with_precision" + randomNameSuffix();

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
        String tableName = "test_invalid_analyze_table" + randomNameSuffix();
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
                "\\Qline 1:52: Invalid value for catalog 'hive' analyze property 'columns': Cannot convert [ARRAY[42]] to array(varchar)\\E");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeUnpartitionedTableWithColumnSubset()
    {
        String tableName = "test_analyze_columns_unpartitioned_table" + randomNameSuffix();
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
        String tableName = "test_analyze_columns_unpartitioned_table_with_empty_column_subset" + randomNameSuffix();
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
        String tableName = "test_drop_stats_partitioned_table" + randomNameSuffix();
        createPartitionedTableForAnalyzeTest(tableName);

        // Run analyze on the entire table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '0', '1'),
                    ('c_double', null, 2.0, 0.5, null, '1.2', '2.2'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '1', '2'),
                    ('c_double', null, 2.0, 0.5, null, '2.3', '3.3'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 1.0, 0.0, null, null, null),
                    ('c_bigint', null, 4.0, 0.0, null, '4', '7'),
                    ('c_double', null, 4.0, 0.0, null, '4.7', '7.7'),
                    ('c_timestamp', null, 4.0, 0.0, null, null, null),
                    ('c_varchar', 16.0, 4.0, 0.0, null, null, null),
                    ('c_varbinary', 8.0, null, 0.0, null, null, null),
                    ('p_varchar', 0.0, 0.0, 1.0, null, null, null),
                    ('p_bigint', 0.0, 0.0, 1.0, null, null, null),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '2', '3'),
                    ('c_double', null, 2.0, 0.5, null, '3.4', '4.4'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '8', '8'),
                    (null, null, null, null, 4.0, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);

        // Drop stats for 2 partitions
        assertUpdate(format("CALL system.drop_stats('%s', '%s', ARRAY[ARRAY['p2', '7'], ARRAY['p3', '8']])", TPCH_SCHEMA, tableName));

        // Note: Even after deleting stats from metastore, stats for partitioned columns will be present since
        // we try to estimate them based on available partition information. This will help engine use ndv, min and max
        // in certain optimizer rules.

        // Only stats for the specified partitions should be removed
        // no change
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 2.0, 0.5, null, null, null),
                    ('c_bigint', null, 2.0, 0.5, null, '0', '1'),
                    ('c_double', null, 2.0, 0.5, null, '1.2', '2.2'),
                    ('c_timestamp', null, 2.0, 0.5, null, null, null),
                    ('c_varchar', 8.0, 2.0, 0.5, null, null, null),
                    ('c_varbinary', 4.0, null, 0.5, null, null, null),
                    ('p_varchar', 8.0, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, 4.0, null, null)
                """);
        // [p2, 7] had stats dropped
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, null, null, null)
                """);
        // no change
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, 1.0, 0.0, null, null, null),
                    ('c_bigint', null, 4.0, 0.0, null, '4', '7'),
                    ('c_double', null, 4.0, 0.0, null, '4.7', '7.7'),
                    ('c_timestamp', null, 4.0, 0.0, null, null, null),
                    ('c_varchar', 16.0, 4.0, 0.0, null, null, null),
                    ('c_varbinary', 8.0, null, 0.0, null, null, null),
                    ('p_varchar', 0.0, 0.0, 1.0, null, null, null),
                    ('p_bigint', 0.0, 0.0, 1.0, null, null, null),
                    (null, null, null, null, 4.0, null, null)
                """);
        // [p3, 8] had stats dropped
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '8', '8'),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, 9, 9),
                    (null, null, null, null, null, null, null)
                """);

        // Drop stats for the entire table
        assertUpdate(format("CALL system.drop_stats('%s', '%s')", TPCH_SCHEMA, tableName));

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '7', '7'),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 0.0, 1.0, null, null, null),
                    ('p_bigint', null, 0.0, 1.0, null, null, null),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '8', '8'),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '9', '9'),
                    (null, null, null, null, null, null, null)
                """);
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 1.0, 0.0, null, null, null),
                    ('p_bigint', null, 1.0, 0.0, null, '9', '9'),
                    (null, null, null, null, null, null, null)
                """);

        // All table stats are gone
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                SELECT * FROM VALUES
                    ('c_boolean', null, null, null, null, null, null),
                    ('c_bigint', null, null, null, null, null, null),
                    ('c_double', null, null, null, null, null, null),
                    ('c_timestamp', null, null, null, null, null, null),
                    ('c_varchar', null, null, null, null, null, null),
                    ('c_varbinary', null, null, null, null, null, null),
                    ('p_varchar', null, 5.0, 0.16666666666666666, null, null, null),
                    ('p_bigint', null, 3.0, 0.16666666666666666, null, '7', '9'),
                    (null, null, null, null, null, null, null)
                """);

        assertUpdate("DROP TABLE\n" + tableName);
    }

    @Test
    public void testDropStatsPartitionedTableWithNullArgument()
    {
        assertQueryFails("CALL system.drop_stats(NULL, 'page_views')", "schema_name cannot be null");
        assertQueryFails("CALL system.drop_stats('web', NULL)", "table_name cannot be null");
    }

    @Test
    public void testDropStatsUnpartitionedTable()
    {
        String tableName = "test_drop_all_stats_unpartitioned_table" + randomNameSuffix();
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
        String tableName = "test_insert_multiple_columns_same_channel" + randomNameSuffix();
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
        String tableName = "test_create_avro_table_with_schema_url" + randomNameSuffix();
        TrinoFileSystem fileSystem = getTrinoFileSystem();
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        String schema =
                """
                {
                    "namespace": "io.trino.test",
                    "name": "camelCase",
                    "type": "record",
                    "fields": [
                       { "name":"stringCol", "type":"string" },
                       { "name":"a", "type":"int" }
                    ]
                }\
                """;
        Location schemaFile = tempDir.appendPath("avro_camelCamelCase_col.avsc");
        try (OutputStream out = fileSystem.newOutputFile(schemaFile).create()) {
            out.write(schema.getBytes(UTF_8));
        }

        String createTableSql = getAvroCreateTableSql(tableName, schemaFile);
        String expectedShowCreateTable = getAvroCreateTableSql(tableName, schemaFile);

        assertUpdate(createTableSql);

        try {
            MaterializedResult actual = computeActual("SHOW CREATE TABLE " + tableName);
            assertThat(actual.getOnlyValue()).isEqualTo(expectedShowCreateTable);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            fileSystem.deleteDirectory(tempDir);
        }
    }

    @Test
    public void testCreateAvroTableWithCamelCaseFieldSchema()
            throws Exception
    {
        String tableName = "test_create_avro_table_with_camelcase_schema_url_" + randomNameSuffix();
        TrinoFileSystem fileSystem = getTrinoFileSystem();
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        String schema =
                """
                {
                    "namespace": "io.trino.test",
                    "name": "camelCase",
                    "type": "record",
                    "fields": [
                       { "name":"stringCol", "type":"string" },
                       { "name":"a", "type":"int" }
                    ]
                }\
                """;
        Location schemaFile = tempDir.appendPath("avro_camelCamelCase_col.avsc");
        try (OutputStream out = fileSystem.newOutputFile(schemaFile).create()) {
            out.write(schema.getBytes(UTF_8));
        }

        String createTableSql = format("CREATE TABLE %s.%s.%s (\n" +
                        "   stringCol varchar,\n" +
                        "   a INT\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = '%s',\n" +
                        "   format = 'AVRO'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                schemaFile);

        assertUpdate(createTableSql);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('hi', 1)", 1);
            assertQuery("SELECT * FROM " + tableName, "SELECT 'hi', 1");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            fileSystem.deleteDirectory(tempDir);
        }
    }

    @Test
    public void testCreateAvroTableWithNestedCamelCaseFieldSchema()
            throws Exception
    {
        String tableName = "test_create_avro_table_with_nested_camelcase_schema_url_" + randomNameSuffix();
        TrinoFileSystem fileSystem = getTrinoFileSystem();
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        String schema =
                """
                {
                    "namespace": "io.trino.test",
                    "name": "camelCaseNested",
                    "type": "record",
                    "fields": [
                        {
                            "name":"nestedRow",
                            "type": ["null", {
                                "namespace": "io.trino.test",
                                 "name": "nestedRecord",
                                 "type": "record",
                                 "fields": [
                                    { "name":"stringCol", "type":"string"},
                                    { "name":"intCol", "type":"int" }
                                ]
                            }]
                        }
                    ]
                 }\
                 """;
        Location schemaFile = tempDir.appendPath("avro_camelCamelCase_col.avsc");
        try (OutputStream out = fileSystem.newOutputFile(schemaFile).create()) {
            out.write(schema.getBytes(UTF_8));
        }

        String createTableSql = format("CREATE TABLE %s.%s.%s (\n" +
                        "   nestedRow ROW(stringCol varchar, intCol int)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = '%s',\n" +
                        "   format = 'AVRO'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                schemaFile);

        assertUpdate(createTableSql);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ROW(ROW('hi', 1))", 1);
            assertQuery("SELECT nestedRow.stringCol FROM " + tableName, "SELECT 'hi'");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            fileSystem.deleteDirectory(tempDir);
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
        String tableName = "test_alter_avro_table_with_schema_url" + randomNameSuffix();
        TrinoFileSystem fileSystem = getTrinoFileSystem();
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        String schema =
                """
                {
                     "namespace": "io.trino.test",
                     "name": "single_column",
                     "type": "record",
                     "fields": [
                        { "name": "string_col", "type":"string" }
                     ]
                }\
                """;
        Location schemaFile = tempDir.appendPath("avro_single_column.avsc");
        try (OutputStream out = fileSystem.newOutputFile(schemaFile).create()) {
            out.write(schema.getBytes(UTF_8));
        }

        assertUpdate(getAvroCreateTableSql(tableName, schemaFile));

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
            fileSystem.deleteDirectory(tempDir);
        }
    }

    private String getAvroCreateTableSql(String tableName, Location schemaFile)
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
    public void testUseSortedProperties()
    {
        String tableName = "test_propagate_table_scan_sorting_properties" + randomNameSuffix();
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

    @Test
    public void testCreateTableWithCompressionCodec()
    {
        for (HiveCompressionCodec compressionCodec : HiveCompressionCodec.values()) {
            testWithAllStorageFormats((session, hiveStorageFormat) -> testCreateTableWithCompressionCodec(session, hiveStorageFormat, compressionCodec));
        }
    }

    private void testCreateTableWithCompressionCodec(Session baseSession, HiveStorageFormat storageFormat, HiveCompressionCodec compressionCodec)
    {
        Session session = Session.builder(baseSession)
                .setCatalogSessionProperty(baseSession.getCatalog().orElseThrow(), "compression_codec", compressionCodec.name())
                .build();
        String tableName = "test_table_with_compression_" + compressionCodec + randomNameSuffix();
        String createTableSql = format("CREATE TABLE %s WITH (format = '%s') AS TABLE tpch.tiny.nation", tableName, storageFormat);
        // TODO (https://github.com/trinodb/trino/issues/9142) Support LZ4 compression with native Parquet writer
        boolean unsupported = (storageFormat == HiveStorageFormat.PARQUET || storageFormat == HiveStorageFormat.AVRO) && compressionCodec == HiveCompressionCodec.LZ4;
        if (unsupported) {
            assertQueryFails(session, createTableSql, "Compression codec " + compressionCodec + " not supported for " + storageFormat.humanName());
            return;
        }
        assertUpdate(session, createTableSql, 25);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * Like {@link #testCreateTableWithCompressionCodec}, but for table with empty buckets. Empty buckets have separate write code path.
     */
    @Test
    public void testCreateTableWithEmptyBucketsAndCompressionCodec()
    {
        for (HiveCompressionCodec compressionCodec : HiveCompressionCodec.values()) {
            testWithAllStorageFormats((session, hiveStorageFormat) -> testCreateTableWithEmptyBucketsAndCompressionCodec(session, hiveStorageFormat, compressionCodec));
        }
    }

    private void testCreateTableWithEmptyBucketsAndCompressionCodec(Session baseSession, HiveStorageFormat storageFormat, HiveCompressionCodec compressionCodec)
    {
        Session session = Session.builder(baseSession)
                .setCatalogSessionProperty(baseSession.getCatalog().orElseThrow(), "compression_codec", compressionCodec.name())
                .build();
        String tableName = "test_table_bucket_with_compression_" + compressionCodec + randomNameSuffix();
        String createTableSql = format("CREATE TABLE %s WITH (format = '%s', bucketed_by = ARRAY['regionkey'], bucket_count = 7) AS TABLE tpch.tiny.nation", tableName, storageFormat);
        // TODO (https://github.com/trinodb/trino/issues/9142) Support LZ4 compression with native Parquet writer
        boolean unsupported = (storageFormat == HiveStorageFormat.PARQUET || storageFormat == HiveStorageFormat.AVRO) && compressionCodec == HiveCompressionCodec.LZ4;
        if (unsupported) {
            assertQueryFails(session, createTableSql, "Compression codec " + compressionCodec + " not supported for " + storageFormat.humanName());
            return;
        }
        assertUpdate(session, createTableSql, 25);
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
        String tableName = "test_select_with_no_columns" + randomNameSuffix();
        @Language("SQL") String createTable = format(
                "CREATE TABLE %s (col0) WITH (format = '%s') AS VALUES 5, 6, 7",
                tableName,
                storageFormat);
        assertUpdate(session, createTable, 3);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();

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
        String tableName = "test_schema_evolution_column_pruning_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
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

        String tableLocation = getTableLocation(tableName);

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
                "\\QIncorrect timestamp precision for timestamp(9); the configured precision is " + HiveTimestampPrecision.MICROSECONDS + "; column name: ts");
        assertQueryFails(
                session,
                "CREATE TABLE test_invalid_precision_timestamp (ts TIMESTAMP(9))",
                "\\QIncorrect timestamp precision for timestamp(9); the configured precision is " + HiveTimestampPrecision.MICROSECONDS + "; column name: ts");
        assertQueryFails(
                session,
                "CREATE TABLE test_invalid_precision_timestamp(ts) AS SELECT TIMESTAMP '2001-02-03 11:22:33.123'",
                "\\QIncorrect timestamp precision for timestamp(3); the configured precision is " + HiveTimestampPrecision.MICROSECONDS + "; column name: ts");
        assertQueryFails(
                session,
                "CREATE TABLE test_invalid_precision_timestamp (ts TIMESTAMP(3))",
                "\\QIncorrect timestamp precision for timestamp(3); the configured precision is " + HiveTimestampPrecision.MICROSECONDS + "; column name: ts");
    }

    @Test
    public void testCoercingVarchar0ToVarchar1()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_coercion_create_table_varchar",
                "(var_column_0 varchar(0), var_column_1 varchar(1), var_column_10 varchar(10))")) {
            assertThat(getColumnType(testTable.getName(), "var_column_0")).isEqualTo("varchar(1)");
            assertThat(getColumnType(testTable.getName(), "var_column_1")).isEqualTo("varchar(1)");
            assertThat(getColumnType(testTable.getName(), "var_column_10")).isEqualTo("varchar(10)");
        }
    }

    @Test
    public void testCoercingVarchar0ToVarchar1WithCTAS()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_coercion_ctas_varchar",
                "AS SELECT '' AS var_column")) {
            assertThat(getColumnType(testTable.getName(), "var_column")).isEqualTo("varchar(1)");
        }
    }

    @Test
    public void testCoercingVarchar0ToVarchar1WithCTASNoData()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_coercion_ctas_nd_varchar",
                "AS SELECT '' AS var_column WITH NO DATA")) {
            assertThat(getColumnType(testTable.getName(), "var_column")).isEqualTo("varchar(1)");
        }
    }

    @Test
    public void testCoercingVarchar0ToVarchar1WithAddColumn()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_coercion_add_column_varchar",
                "(col integer)")) {
            assertUpdate("ALTER TABLE " + testTable.getName() + " ADD COLUMN var_column varchar(0)");
            assertThat(getColumnType(testTable.getName(), "var_column")).isEqualTo("varchar(1)");
        }
    }

    @Test
    public void testOptimize()
    {
        String tableName = "test_optimize_" + randomNameSuffix();
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

        // optimize with delimited procedure name
        assertQueryFails(optimizeEnabledSession, "ALTER TABLE " + tableName + " EXECUTE \"optimize\"", "Table procedure not registered: optimize");
        assertUpdate(optimizeEnabledSession, "ALTER TABLE " + tableName + " EXECUTE \"OPTIMIZE\"");
        // optimize with delimited parameter name (and procedure name)
        assertUpdate(optimizeEnabledSession, "ALTER TABLE " + tableName + " EXECUTE \"OPTIMIZE\" (\"file_size_threshold\" => '10B')"); // TODO (https://github.com/trinodb/trino/issues/11326) this should fail
        assertUpdate(optimizeEnabledSession, "ALTER TABLE " + tableName + " EXECUTE \"OPTIMIZE\" (\"FILE_SIZE_THRESHOLD\" => '10B')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeWithWriterScaling()
    {
        testOptimizeWithWriterScaling(true, false, DataSize.of(1, GIGABYTE));
        testOptimizeWithWriterScaling(false, true, DataSize.of(0, MEGABYTE));
    }

    private void testOptimizeWithWriterScaling(boolean scaleWriters, boolean taskScaleWritersEnabled, DataSize writerScalingMinDataProcessed)
    {
        String tableName = "test_optimize_witer_scaling" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation WITH NO DATA", 0);

        insertNationNTimes(tableName, 4);
        assertNationNTimes(tableName, 4);

        Set<String> initialFiles = getTableFiles(tableName);
        assertThat(initialFiles).hasSize(4);

        Session.SessionBuilder writerScalingSessionBuilder = Session.builder(optimizeEnabledSession())
                .setSystemProperty("scale_writers", String.valueOf(scaleWriters))
                .setSystemProperty("writer_scaling_min_data_processed", writerScalingMinDataProcessed.toString())
                // task_scale_writers_enabled shouldn't have any effect on writing data in the optimize command
                .setSystemProperty("task_scale_writers_enabled", String.valueOf(taskScaleWritersEnabled))
                .setSystemProperty("task_min_writer_count", "1");

        if (!scaleWriters) {
            writerScalingSessionBuilder.setSystemProperty("max_writer_task_count", "1");
        }

        assertUpdate(writerScalingSessionBuilder.build(), "ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB')");
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

        String tableName = "test_optimize_with_partitioning_" + randomNameSuffix();
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
                .setSystemProperty("writer_scaling_min_data_processed", "100GB")
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
        String tableName = "test_optimize_with_bucketing_" + randomNameSuffix();
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
        String tableName = "test_optimize_system_table_" + randomNameSuffix();
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
        for (int i = 0; i < times; i++) {
            assertUpdate("INSERT INTO " + tableName + "(nationkey, name, regionkey, comment) SELECT * FROM tpch.sf1.nation", 25);
        }
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

        String tableName = "test_timestamp_precision_" + randomNameSuffix();
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
        testWithAllStorageFormats(this::testTimestampPrecisionCtas);
    }

    private void testTimestampPrecisionCtas(Session session, HiveStorageFormat storageFormat)
    {
        if (storageFormat == HiveStorageFormat.AVRO) {
            // Avro timestamps are stored with millisecond precision
            return;
        }

        String tableName = "test_timestamp_precision_" + randomNameSuffix();
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
        String viewName = "select_from_view_without_catalog_and_schema_" + randomNameSuffix();
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
        String tableName = "ts_hive_table_" + randomNameSuffix();
        assertUpdate(
                withTimestampPrecision(defaultSession, HiveTimestampPrecision.NANOSECONDS),
                "CREATE TABLE " + tableName + " AS SELECT TIMESTAMP '1990-01-02 12:13:14.123456789' ts",
                1);

        // Presto view created with config property set to MILLIS and session property not set
        String prestoViewNameDefault = "presto_view_ts_default_" + randomNameSuffix();
        assertUpdate(defaultSession, "CREATE VIEW " + prestoViewNameDefault + " AS SELECT *  FROM " + tableName);

        assertThat(query(defaultSession, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(defaultSession, "SELECT ts  FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(millisSession, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");
        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        // Presto view created with config property set to MILLIS and session property set to NANOS
        String prestoViewNameNanos = "presto_view_ts_nanos_" + randomNameSuffix();
        assertUpdate(nanosSessions, "CREATE VIEW " + prestoViewNameNanos + " AS SELECT *  FROM " + tableName);

        assertThat(query(defaultSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(defaultSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(millisSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        testTimestampWithTimeZone(HiveTimestampPrecision.MILLISECONDS);
        testTimestampWithTimeZone(HiveTimestampPrecision.MICROSECONDS);
        testTimestampWithTimeZone(HiveTimestampPrecision.NANOSECONDS);
    }

    private void testTimestampWithTimeZone(HiveTimestampPrecision timestampPrecision)
    {
        assertUpdate(withTimestampPrecision(getSession(), HiveTimestampPrecision.NANOSECONDS), "CREATE TABLE test_timestamptz_base (t timestamp(9)) WITH (format = 'PARQUET')");
        for (HiveTimestampPrecision precision : HiveTimestampPrecision.values()) {
            long fractionalPart = switch (precision) {
                case MILLISECONDS -> 123;
                case MICROSECONDS -> 123456;
                case NANOSECONDS -> 123456789;
            };
            assertUpdate(
                    withTimestampPrecision(getSession(), HiveTimestampPrecision.NANOSECONDS),
                    "INSERT INTO test_timestamptz_base (t) VALUES" +
                            "(timestamp '2022-07-26 12:13:14." + fractionalPart + "')", 1);
        }

        // Writing TIMESTAMP WITH LOCAL TIME ZONE is not supported, so we first create Parquet object by writing unzoned
        // timestamp (which is converted to UTC using default timezone) and then creating another table that reads from the same file.
        String tableLocation = getTableLocation("test_timestamptz_base");

        Session session = withTimestampPrecision(getSession(), timestampPrecision);
        String catalog = session.getCatalog().orElseThrow();
        // TIMESTAMP WITH LOCAL TIME ZONE is not mapped to any Trino type, so we need to create the metastore entry manually
        HiveMetastore metastore = getConnectorService(getDistributedQueryRunner(), HiveMetastoreFactory.class)
                .createMetastore(Optional.of(session.getIdentity().toConnectorIdentity(catalog)));
        metastore.createTable(
                new Table(
                        "tpch",
                        "test_timestamptz",
                        Optional.of("hive"),
                        "EXTERNAL_TABLE",
                        new Storage(
                                HiveStorageFormat.PARQUET.toStorageFormat(),
                                Optional.of(tableLocation),
                                Optional.empty(),
                                false,
                                Collections.emptyMap()),
                        List.of(new Column("t", HiveType.HIVE_TIMESTAMPLOCALTZ, Optional.empty(), Map.of())),
                        List.of(),
                        Collections.emptyMap(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                PrincipalPrivileges.fromHivePrivilegeInfos(Collections.emptySet()));

        long microsFraction = switch (timestampPrecision) {
            case MILLISECONDS -> 123;
            case MICROSECONDS, NANOSECONDS -> 123456;
        };
        long nanosFraction = switch (timestampPrecision) {
            case MILLISECONDS -> 123;
            case MICROSECONDS -> 123457;
            case NANOSECONDS -> 123456789;
        };
        assertThat(query(session, "SELECT * FROM test_timestamptz"))
                .matches("VALUES " +
                        "(TIMESTAMP '2022-07-26 17:13:14.123 UTC')," +
                        "(TIMESTAMP '2022-07-26 17:13:14." + microsFraction + " UTC')," +
                        "(TIMESTAMP '2022-07-26 17:13:14." + nanosFraction + " UTC')");

        assertUpdate("DROP TABLE test_timestamptz");
        assertUpdate("DROP TABLE test_timestamptz_base");
    }

    @Test
    public void testUseColumnNames()
    {
        testUseColumnNames(HiveStorageFormat.ORC, true);
        testUseColumnNames(HiveStorageFormat.ORC, false);
        testUseColumnNames(HiveStorageFormat.PARQUET, true);
        testUseColumnNames(HiveStorageFormat.PARQUET, false);
        testUseColumnNames(HiveStorageFormat.AVRO, false);
        testUseColumnNames(HiveStorageFormat.JSON, false);
        testUseColumnNames(HiveStorageFormat.RCBINARY, false);
        testUseColumnNames(HiveStorageFormat.RCTEXT, false);
        testUseColumnNames(HiveStorageFormat.SEQUENCEFILE, false);
        testUseColumnNames(HiveStorageFormat.TEXTFILE, false);
    }

    private void testUseColumnNames(HiveStorageFormat format, boolean formatUseColumnNames)
    {
        String lowerCaseFormat = format.name().toLowerCase(Locale.ROOT);
        Session.SessionBuilder builder = Session.builder(getSession());
        if (format == HiveStorageFormat.ORC || format == HiveStorageFormat.PARQUET) {
            builder.setCatalogSessionProperty(catalog, lowerCaseFormat + "_use_column_names", String.valueOf(formatUseColumnNames));
        }
        Session admin = builder.build();
        String tableName = format("test_renames_%s_%s_%s", lowerCaseFormat, formatUseColumnNames, randomNameSuffix());
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

    @Test
    public void testHiddenColumnNameConflict()
    {
        testHiddenColumnNameConflict("$path");
        testHiddenColumnNameConflict("$bucket");
        testHiddenColumnNameConflict("$file_size");
        testHiddenColumnNameConflict("$file_modified_time");
        testHiddenColumnNameConflict("$partition");
    }

    private void testHiddenColumnNameConflict(String columnName)
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_hidden_column_name_conflict",
                format("(\"%s\" int, _bucket int, _partition int) WITH (partitioned_by = ARRAY['_partition'], bucketed_by = ARRAY['_bucket'], bucket_count = 10)", columnName))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .nonTrinoExceptionFailure().hasMessageContaining("Multiple entries with same key: " + columnName);
        }
    }

    @Test
    public void testUseColumnAddDrop()
    {
        testUseColumnAddDrop(HiveStorageFormat.ORC, true);
        testUseColumnAddDrop(HiveStorageFormat.ORC, false);
        testUseColumnAddDrop(HiveStorageFormat.PARQUET, true);
        testUseColumnAddDrop(HiveStorageFormat.PARQUET, false);
        testUseColumnAddDrop(HiveStorageFormat.AVRO, false);
        testUseColumnAddDrop(HiveStorageFormat.JSON, false);
        testUseColumnAddDrop(HiveStorageFormat.RCBINARY, false);
        testUseColumnAddDrop(HiveStorageFormat.RCTEXT, false);
        testUseColumnAddDrop(HiveStorageFormat.SEQUENCEFILE, false);
        testUseColumnAddDrop(HiveStorageFormat.TEXTFILE, false);
    }

    private void testUseColumnAddDrop(HiveStorageFormat format, boolean formatUseColumnNames)
    {
        String lowerCaseFormat = format.name().toLowerCase(Locale.ROOT);
        Session.SessionBuilder builder = Session.builder(getSession());
        if (format == HiveStorageFormat.ORC || format == HiveStorageFormat.PARQUET) {
            builder.setCatalogSessionProperty(catalog, lowerCaseFormat + "_use_column_names", String.valueOf(formatUseColumnNames));
        }
        Session admin = builder.build();
        String tableName = format("test_add_drop_%s_%s_%s", lowerCaseFormat, formatUseColumnNames, randomNameSuffix());
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
        assertThat(getOnlyElement(result.getOnlyColumnAsSet())).isEqualTo(getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testAutoPurgeProperty()
    {
        String tableName = "test_auto_purge_property" + randomNameSuffix();
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s " +
                        "AS " +
                        "SELECT * FROM tpch.tiny.customer",
                tableName);
        assertUpdate(createTableSql, 1500L);

        TableMetadata tableMetadataDefaults = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertThat(tableMetadataDefaults.metadata().getProperties()).doesNotContainKey(AUTO_PURGE);

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
        assertThat(tableMetadataWithPurge.metadata().getProperties()).containsEntry(AUTO_PURGE, true);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testExplainAnalyzePhysicalInputSize()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT * FROM nation a",
                "Physical input: .*B");
    }

    @Test
    public void testExplainAnalyzePhysicalReadWallTime()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT * FROM nation a",
                "Physical input time: .*s");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT * FROM nation WHERE nationkey > 1",
                "Physical input time: .*s");
    }

    @Test
    public void testExplainAnalyzeScanFilterProjectWallTime()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT nationkey * 2 FROM nation WHERE nationkey > 0",
                "'Filter CPU time' = \\{duration=.*}",
                "'Projection CPU time' = \\{duration=.*}");
    }

    @Test
    public void testExplainAnalyzeFilterProjectWallTime()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT * FROM (SELECT nationkey, count(*) cnt FROM nation GROUP BY 1) where cnt > 0",
                "'Filter CPU time' = \\{duration=.*}",
                "'Projection CPU time' = \\{duration=.*}");
    }

    @Test
    public void testExplainAnalyzeAccumulatorUpdateWallTime()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT count(*) FROM nation",
                "'Accumulator update CPU time' = \\{duration=.*}");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT name, (SELECT max(name) FROM region WHERE regionkey > nation.regionkey) FROM nation",
                "'Accumulator update CPU time' = \\{duration=.*}");
    }

    @Test
    public void testExplainAnalyzeGroupByHashUpdateWallTime()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT nationkey FROM nation GROUP BY nationkey",
                "'Group by hash update CPU time' = \\{duration=.*}");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT count(*), nationkey FROM nation GROUP BY nationkey",
                "'Accumulator update CPU time' = \\{duration=.*}",
                "'Group by hash update CPU time' = \\{duration=.*}");
    }

    @Test
    public void testCreateAcidTableUnsupported()
    {
        assertQueryFails("CREATE TABLE acid_unsupported (x int) WITH (transactional = true)", "FileHiveMetastore does not support ACID tables");
        assertQueryFails("CREATE TABLE acid_unsupported WITH (transactional = true) AS SELECT 123 x", "FileHiveMetastore does not support ACID tables");
    }

    @Test
    public void testExtraProperties()
    {
        String tableName = "create_table_with_multiple_extra_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE %s (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.two'], ARRAY['one', 'two']))".formatted(tableName));

        assertQuery(
                "SELECT \"extra.property.one\", \"extra.property.two\" FROM \"%s$properties\"".formatted(tableName),
                "SELECT 'one', 'two'");
        assertThat(computeActual("SHOW CREATE TABLE %s".formatted(tableName)).getOnlyValue())
                .isEqualTo("CREATE TABLE hive.tpch.%s (\n".formatted(tableName) +
                        "   c1 integer\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")");
        assertUpdate("DROP TABLE %s".formatted(tableName));
    }

    @Test
    public void testExtraPropertiesWithCtas()
    {
        String tableName = "create_table_ctas_with_multiple_extra_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE %s (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.two'], ARRAY['one', 'two']))".formatted(tableName));

        assertQuery(
                "SELECT \"extra.property.one\", \"extra.property.two\" FROM \"%s$properties\"".formatted(tableName),
                "SELECT 'one', 'two'");
        assertThat(computeActual("SHOW CREATE TABLE %s".formatted(tableName)).getOnlyValue())
                .isEqualTo("CREATE TABLE hive.tpch.%s (\n".formatted(tableName) +
                        "   c1 integer\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")");

        assertUpdate("DROP TABLE %s".formatted(tableName));
    }

    @Test
    public void testShowCreateWithExtraProperties()
    {
        String tableName = format("%s.%s.show_create_table_with_extra_properties_%s", getSession().getCatalog().get(), getSession().getSchema().get(), randomNameSuffix());
        assertUpdate("CREATE TABLE %s (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.two'], ARRAY['one', 'two']))".formatted(tableName));

        assertThat(computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                .isEqualTo("CREATE TABLE %s (\n".formatted(tableName) +
                        "   c1 integer\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")");

        assertUpdate("DROP TABLE %s".formatted(tableName));
    }

    @Test
    public void testDuplicateExtraProperties()
    {
        assertQueryFails(
                "CREATE TABLE create_table_with_duplicate_extra_properties (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property', 'extra.property'], ARRAY['true', 'false']))",
                "line 1:78: Invalid value for catalog 'hive' table property 'extra_properties': Cannot convert.*");
        assertQueryFails(
                "CREATE TABLE create_table_select_as_with_duplicate_extra_properties (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property', 'extra.property'], ARRAY['true', 'false']))",
                "line 1:88: Invalid value for catalog 'hive' table property 'extra_properties': Cannot convert.*");
    }

    @Test
    public void testOverwriteExistingPropertyWithExtraProperties()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE create_table_with_overwrite_extra_properties (c1 integer) WITH (extra_properties = MAP(ARRAY['transactional'], ARRAY['true']))"))
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Illegal keys in extra_properties: [transactional]");

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE create_table_as_select_with_extra_properties WITH (extra_properties = MAP(ARRAY['rawDataSize'], ARRAY['1'])) AS SELECT 1 as c1"))
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Illegal keys in extra_properties: [rawDataSize]");
    }

    @Test
    public void testNullExtraProperty()
    {
        assertQueryFails(
                "CREATE TABLE create_table_with_duplicate_extra_properties (c1 integer) WITH (extra_properties = MAP(ARRAY['null.property'], ARRAY[null]))",
                ".*Extra table property value cannot be null '\\{null.property=null}'.*");
        assertQueryFails(
                "CREATE TABLE create_table_as_select_with_extra_properties WITH (extra_properties = MAP(ARRAY['null.property'], ARRAY[null])) AS SELECT 1 as c1",
                ".*Extra table property value cannot be null '\\{null.property=null}'.*");
    }

    @Test
    public void testCollidingMixedCaseProperty()
    {
        String tableName = "create_table_with_mixed_case_extra_properties" + randomNameSuffix();

        assertUpdate("CREATE TABLE %s (c1 integer) WITH (extra_properties = MAP(ARRAY['one', 'ONE'], ARRAY['one', 'ONE']))".formatted(tableName));
        // TODO: (https://github.com/trinodb/trino/issues/17) This should run successfully
        assertThat(query("SELECT * FROM \"%s$properties\"".formatted(tableName)))
                .nonTrinoExceptionFailure().hasMessageContaining("Multiple entries with same key: one=one and one=one");

        assertUpdate("DROP TABLE %s".formatted(tableName));
    }

    @Test
    public void testExtraPropertiesOnView()
    {
        String tableName = "create_view_with_multiple_extra_properties_" + randomNameSuffix();
        assertUpdate("CREATE VIEW %s WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.two'], ARRAY['one', 'two'])) AS SELECT 1 as colA".formatted(tableName));

        assertQuery(
                "SELECT \"extra.property.one\", \"extra.property.two\" FROM \"%s$properties\"".formatted(tableName),
                "SELECT 'one', 'two'");
        assertThat(computeActual("SHOW CREATE VIEW %s".formatted(tableName)).getOnlyValue())
                .isEqualTo(
                        """
                        CREATE VIEW hive.tpch.%s SECURITY DEFINER AS
                        SELECT 1 colA\
                        """.formatted(tableName));
        assertUpdate("DROP VIEW %s".formatted(tableName));
    }

    @Test
    public void testDuplicateExtraPropertiesOnView()
    {
        assertQueryFails(
                "CREATE VIEW create_view_with_duplicate_extra_properties WITH (extra_properties = MAP(ARRAY['extra.property', 'extra.property'], ARRAY['true', 'false'])) AS SELECT 1 as colA",
                "line 1:63: Invalid value for catalog 'hive' view property 'extra_properties': Cannot convert.*");
    }

    @Test
    public void testNullExtraPropertyOnView()
    {
        assertQueryFails(
                "CREATE VIEW create_view_with_duplicate_extra_properties WITH (extra_properties = MAP(ARRAY['null.property'], ARRAY[null])) AS SELECT 1 as c1",
                ".*Extra view property value cannot be null '\\{null.property=null}'.*");
    }

    @Test
    public void testCollidingMixedCasePropertyOnView()
    {
        String tableName = "create_view_with_mixed_case_extra_properties" + randomNameSuffix();

        assertUpdate("CREATE VIEW %s WITH (extra_properties = MAP(ARRAY['one', 'ONE'], ARRAY['one', 'ONE'])) AS SELECT 1 as colA".formatted(tableName));
        // TODO: (https://github.com/trinodb/trino/issues/17) This should run successfully
        assertThat(query("SELECT * FROM \"%s$properties\"".formatted(tableName)))
                .nonTrinoExceptionFailure().hasMessageContaining("Multiple entries with same key: one=one and one=one");

        assertUpdate("DROP VIEW %s".formatted(tableName));
    }

    @Test
    public void testCreateViewWithTableProperties()
    {
        assertQueryFails(
                "CREATE VIEW create_view_with_table_properties WITH (format = 'ORC', extra_properties = MAP(ARRAY['extra.property'], ARRAY['true'])) AS SELECT 1 as colA",
                "line 1:53: Catalog 'hive' view property 'format' does not exist");
    }

    @Test
    public void testCreateViewWithPreDefinedPropertiesAsExtraProperties()
    {
        assertQueryFails(
                "CREATE VIEW create_view_with_predefined_view_properties WITH (extra_properties = MAP(ARRAY['%s'], ARRAY['true'])) AS SELECT 1 as colA".formatted(TABLE_COMMENT),
                "Illegal keys in extra_properties: \\[comment]");

        assertQueryFails(
                "CREATE VIEW create_view_with_predefined_view_properties WITH (extra_properties = MAP(ARRAY['%s'], ARRAY['true'])) AS SELECT 1 as colA".formatted(PRESTO_VIEW_FLAG),
                "Illegal keys in extra_properties: \\[presto_view]");

        assertQueryFails("CREATE VIEW create_view_with_predefined_view_properties WITH (extra_properties = MAP(ARRAY['%s'], ARRAY['true'])) AS SELECT 1 as colA".formatted(TRINO_CREATED_BY),
                "Illegal keys in extra_properties: \\[trino_created_by]");

        assertQueryFails("CREATE VIEW create_view_with_predefined_view_properties WITH (extra_properties = MAP(ARRAY['%s'], ARRAY['true'])) AS SELECT 1 as colA".formatted(TRINO_VERSION_NAME),
                "Illegal keys in extra_properties: \\[trino_version]");

        assertQueryFails(
                "CREATE VIEW create_view_with_predefined_view_properties WITH (extra_properties = MAP(ARRAY['%s'], ARRAY['true'])) AS SELECT 1 as colA".formatted(TRINO_QUERY_ID_NAME),
                "Illegal keys in extra_properties: \\[trino_query_id]");
    }

    @Test
    public void testCommentWithPartitionedTable()
    {
        String table = "test_comment_with_partitioned_table_" + randomNameSuffix();

        assertUpdate(
                """
                CREATE TABLE hive.tpch.%s (
                   regular_column date COMMENT 'regular column comment',
                   partition_column date COMMENT 'partition column comment'
                )
                COMMENT 'table comment'
                WITH (
                   partitioned_by = ARRAY['partition_column']
                )
                """.formatted(table));

        assertThat(getColumnComment(table, "regular_column")).isEqualTo("regular column comment");
        assertThat(getColumnComment(table, "partition_column")).isEqualTo("partition column comment");
        assertThat(getTableComment("hive", "tpch", table)).isEqualTo("table comment");

        assertUpdate("COMMENT ON COLUMN %s.regular_column IS 'new regular column comment'".formatted(table));
        assertThat(getColumnComment(table, "regular_column")).isEqualTo("new regular column comment");
        assertUpdate("COMMENT ON COLUMN %s.partition_column IS 'new partition column comment'".formatted(table));
        assertThat(getColumnComment(table, "partition_column")).isEqualTo("new partition column comment");
        assertUpdate("COMMENT ON TABLE %s IS 'new table comment'".formatted(table));
        assertThat(getTableComment("hive", "tpch", table)).isEqualTo("new table comment");

        assertUpdate("COMMENT ON COLUMN %s.regular_column IS ''".formatted(table));
        assertThat(getColumnComment(table, "regular_column")).isEmpty();
        assertUpdate("COMMENT ON COLUMN %s.partition_column IS ''".formatted(table));
        assertThat(getColumnComment(table, "partition_column")).isEmpty();
        assertUpdate("COMMENT ON TABLE %s IS ''".formatted(table));
        assertThat(getTableComment("hive", "tpch", table)).isEmpty();

        assertUpdate("COMMENT ON COLUMN %s.regular_column IS NULL".formatted(table));
        assertThat(getColumnComment(table, "regular_column")).isNull();
        assertUpdate("COMMENT ON COLUMN %s.partition_column IS NULL".formatted(table));
        assertThat(getColumnComment(table, "partition_column")).isNull();
        assertUpdate("COMMENT ON TABLE %s IS NULL".formatted(table));
        assertThat(getTableComment("hive", "tpch", table)).isNull();

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testSelectWithShortZoneId()
            throws IOException
    {
        URL resourceLocation = Resources.getResource("with_short_zone_id/data/data.orc");

        TrinoFileSystem fileSystem = getTrinoFileSystem();
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("data.orc");
        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_select_with_short_zone_id_",
                "(id INT, firstName VARCHAR, lastName VARCHAR) WITH (external_location = '%s')".formatted(tempDir))) {
            assertThat(query("SELECT * FROM %s".formatted(testTable.getName())))
                    .failure()
                    .hasMessageMatching(".*Failed to read ORC file: .*")
                    .hasStackTraceContaining("Unknown time-zone ID: EST");
        }
    }

    @Test
    public void testFlushMetadataDisabled()
    {
        // Flushing metadata cache does not fail even if cache is disabled
        assertQuerySucceeds("CALL system.flush_metadata_cache()");
    }

    private static final Set<HiveStorageFormat> NAMED_COLUMN_ONLY_FORMATS = ImmutableSet.of(HiveStorageFormat.AVRO, HiveStorageFormat.JSON);

    private Session getParallelWriteSession(Session baseSession)
    {
        return Session.builder(baseSession)
                .setSystemProperty("task_min_writer_count", "4")
                .setSystemProperty("task_max_writer_count", "4")
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        assertOneNotNullResult(getSession(), query);
    }

    private void assertOneNotNullResult(Session session, @Language("SQL") String query)
    {
        MaterializedResult results = getQueryRunner().execute(session, query).toTestTypes();
        assertThat(results.getRowCount()).isEqualTo(1);
        assertThat(results.getMaterializedRows().get(0).getFieldCount()).isEqualTo(1);
        assertThat(results.getMaterializedRows().get(0).getField(0)).isNotNull();
    }

    private Type canonicalizeType(Type type)
    {
        return TESTING_TYPE_MANAGER.getType(getTypeSignature(toHiveType(type)));
    }

    private void assertColumnType(TableMetadata tableMetadata, String columnName, Type expectedType)
    {
        assertThat(tableMetadata.column(columnName).getType()).isEqualTo(canonicalizeType(expectedType));
    }

    private void assertConstraints(@Language("SQL") String query, Set<ColumnConstraint> expected)
    {
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        Set<ColumnConstraint> constraints = getIoPlanCodec().fromJson((String) getOnlyElement(result.getOnlyColumnAsSet()))
                .getInputTableColumnInfos().stream()
                .collect(onlyElement())
                .getConstraint()
                .getColumnConstraints();

        assertThat(constraints).containsAll(expected);
    }

    private void verifyPartition(boolean hasPartition, TableMetadata tableMetadata, List<String> partitionKeys)
    {
        Object partitionByProperty = tableMetadata.metadata().getProperties().get(PARTITIONED_BY_PROPERTY);
        if (hasPartition) {
            assertThat(partitionByProperty).isEqualTo(partitionKeys);
            for (ColumnMetadata columnMetadata : tableMetadata.columns()) {
                boolean partitionKey = partitionKeys.contains(columnMetadata.getName());
                assertThat(columnMetadata.getExtraInfo()).isEqualTo(columnExtraInfo(partitionKey));
            }
        }
        else {
            assertThat(partitionByProperty).isNull();
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
        Session session = storageFormat.session();
        try {
            test.accept(session, storageFormat.format());
        }
        catch (Exception | AssertionError e) {
            throw new AssertionError(format("Failure for format %s with properties %s", storageFormat.format(), session.getCatalogProperties()), e);
        }
    }

    private List<TestingHiveStorageFormat> getAllTestingHiveStorageFormat()
    {
        ImmutableList.Builder<TestingHiveStorageFormat> formats = ImmutableList.builder();
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            if (hiveStorageFormat == HiveStorageFormat.CSV) {
                // CSV supports only unbounded VARCHAR type
                continue;
            }
            if (hiveStorageFormat == REGEX) {
                // REGEX format is read-only
                continue;
            }

            formats.add(new TestingHiveStorageFormat(getSession(), hiveStorageFormat));
        }
        return formats.build();
    }

    private JsonCodec<IoPlan> getIoPlanCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(getQueryRunner().getPlannerContext().getTypeManager())));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(IoPlan.class);
    }

    private record TestingHiveStorageFormat(Session session, HiveStorageFormat format)
    {
        private TestingHiveStorageFormat
        {
            requireNonNull(session, "session is null");
            requireNonNull(format, "format is null");
        }
    }

    private record TypeAndEstimate(Type type, EstimatedStatsAndCost estimate)
    {
        private TypeAndEstimate
        {
            requireNonNull(type, "type is null");
            requireNonNull(estimate, "estimate is null");
        }
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        if (typeName.equals("timestamp(6)")) {
            // It's supported depending on hive timestamp precision configuration, so the exception message doesn't match the expected for asUnsupported().
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("Hive connector does not support column default values");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // This value depends on metastore type
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Schema name must be shorter than or equal to '128' characters but got '129'");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // This value depends on metastore type
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Table name must be shorter than or equal to '128' characters but got .*");
    }

    private Session withTimestampPrecision(Session session, HiveTimestampPrecision precision)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, "timestamp_precision", precision.name())
                .build();
    }

    private String getTableLocation(String tableName)
    {
        return (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);
    }

    private TrinoFileSystem getTrinoFileSystem()
    {
        return getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"));
    }

    @Override
    protected boolean supportsPhysicalPushdown()
    {
        // Hive table is created using default format which is ORC. Currently ORC reader has issue
        // pruning dereferenced struct fields https://github.com/trinodb/trino/issues/17201
        return false;
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_small_file_threshold", "0B")
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_tiny_stripe_threshold", "0B")
                .build();
    }
}
