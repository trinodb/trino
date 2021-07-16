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
package io.trino.sql.rewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.SystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.SqlFormatterUtil;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite.Rewrite;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.Statement;
import io.trino.testing.TestingMetadata;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatementRewrite
{
    private static final String TPCH_CATALOG = "tpch";
    private static final CatalogName TPCH_CATALOG_NAME = new CatalogName(TPCH_CATALOG);
    private static final Session.SessionBuilder CLIENT_SESSION_BUILDER = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .addPreparedStatement("q1", "SELECT a, ? as col2 FROM tpch.s1.t1");
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();

    private TransactionManager transactionManager;
    private AccessControlManager accessControl;
    private Metadata metadata;

    @Test
    public void testDescribeOutputFormatSql()
    {
        assertFormatSql(
                new DescribeOutput(identifier("q1")),
                new DescribeOutputRewrite(),
                "SELECT\n" +
                        "  \"Column Name\"\n" +
                        ", \"Catalog\"\n" +
                        ", \"Schema\"\n" +
                        ", \"Table\"\n" +
                        ", \"Type\"\n" +
                        ", \"Type Size\"\n" +
                        ", \"Aliased\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('a', 'tpch', 's1', 't1', 'bigint', 8, false)\n" +
                        "   , ROW ('col2', '', '', '', 'unknown', 1, true)\n" +
                        ")  \"Statement Output\" (\"Column Name\", \"Catalog\", \"Schema\", \"Table\", \"Type\", \"Type Size\", \"Aliased\")\n");
    }

    @Test
    public void testDescribeInputFormatSql()
    {
        assertFormatSql(
                new DescribeInput(identifier("q1")),
                new DescribeInputRewrite(),
                "SELECT\n" +
                        "  \"Position\"\n" +
                        ", \"Type\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW (0, 'unknown')\n" +
                        ")  \"Parameter Input\" (\"Position\", \"Type\")\n" +
                        "ORDER BY Position ASC\n");
    }

    @Test
    public void testShowSessionFormSql()
    {
        assertFormatSql(
                new ShowSession(Optional.of("%"), Optional.of("$")),
                new ShowQueriesRewrite(),
                "SELECT\n" +
                        "  \"name\" \"Name\"\n" +
                        ", \"value\" \"Value\"\n" +
                        ", \"default\" \"Default\"\n" +
                        ", \"type\" \"Type\"\n" +
                        ", \"description\" \"Description\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('aggregation_operator_unspill_memory_limit', '4MB', '4MB', 'varchar', 'How much memory should be allocated per aggregation operator in unspilling process', true)\n" +
                        "   , ROW ('collect_plan_statistics_for_all_queries', 'false', 'false', 'boolean', 'Collect plan statistics for non-EXPLAIN queries', true)\n" +
                        "   , ROW ('colocated_join', 'false', 'false', 'boolean', 'Experimental: Use a colocated join when possible', true)\n" +
                        "   , ROW ('concurrent_lifespans_per_task', '0', '0', 'integer', 'Experimental: Run a fixed number of groups concurrently for eligible JOINs', true)\n" +
                        "   , ROW ('default_filter_factor_enabled', 'false', 'false', 'boolean', 'use a default filter factor for unknown filters in a filter node', true)\n" +
                        "   , ROW ('dictionary_aggregation', 'false', 'false', 'boolean', 'Enable optimization for aggregations on dictionaries', true)\n" +
                        "   , ROW ('distributed_index_join', 'false', 'false', 'boolean', 'Distribute index joins on join keys instead of executing inline', true)\n" +
                        "   , ROW ('distributed_sort', 'true', 'true', 'boolean', 'Parallelize sort across multiple nodes', true)\n" +
                        "   , ROW ('dynamic_schedule_for_grouped_execution', 'false', 'false', 'boolean', 'Experimental: Use dynamic schedule for grouped execution when possible', true)\n" +
                        "   , ROW ('enable_coordinator_dynamic_filters_distribution', 'true', 'true', 'boolean', 'Enable distribution of dynamic filters from coordinator to all workers', true)\n" +
                        "   , ROW ('enable_dynamic_filtering', 'true', 'true', 'boolean', 'Enable dynamic filtering', true)\n" +
                        "   , ROW ('enable_intermediate_aggregations', 'false', 'false', 'boolean', 'Enable the use of intermediate aggregations', true)\n" +
                        "   , ROW ('enable_large_dynamic_filters', 'false', 'false', 'boolean', 'Enable collection of large dynamic filters', true)\n" +
                        "   , ROW ('enable_stats_calculator', 'true', 'true', 'boolean', 'Enable statistics calculator', true)\n" +
                        "   , ROW ('exchange_compression', 'false', 'false', 'boolean', 'Enable compression in exchanges', true)\n" +
                        "   , ROW ('execution_policy', 'all-at-once', 'all-at-once', 'varchar', 'Policy used for scheduling query tasks', true)\n" +
                        "   , ROW ('filter_and_project_min_output_page_row_count', '256', '256', 'integer', 'Experimental: Minimum output page row count for filter and project operators', true)\n" +
                        "   , ROW ('filter_and_project_min_output_page_size', '500kB', '500kB', 'varchar', 'Experimental: Minimum output page size for filter and project operators', true)\n" +
                        "   , ROW ('grouped_execution', 'false', 'false', 'boolean', 'Use grouped execution when possible', true)\n" +
                        "   , ROW ('hash_partition_count', '100', '100', 'integer', 'Number of partitions for distributed joins and aggregations', true)\n" +
                        "   , ROW ('ignore_downstream_preferences', 'false', 'false', 'boolean', 'Ignore Parent''s PreferredProperties in AddExchange optimizer', true)\n" +
                        "   , ROW ('ignore_stats_calculator_failures', 'false', 'true', 'boolean', 'Ignore statistics calculator failures', true)\n" +
                        "   , ROW ('initial_splits_per_node', '16', '16', 'integer', 'The number of splits each node will run per task, initially', true)\n" +
                        "   , ROW ('iterative_optimizer_timeout', '3.00m', '3.00m', 'varchar', 'Timeout for plan optimization in iterative optimizer', true)\n" +
                        "   , ROW ('iterative_rule_based_column_pruning', 'true', 'true', 'boolean', 'Use iterative rules to prune unreferenced columns', true)\n" +
                        "   , ROW ('join_distribution_type', 'AUTOMATIC', 'AUTOMATIC', 'varchar', 'Join distribution type. Possible values: [BROADCAST, PARTITIONED, AUTOMATIC]', true)\n" +
                        "   , ROW ('join_max_broadcast_table_size', '100MB', '100MB', 'varchar', 'Maximum estimated size of a table that can be broadcast when using automatic join type selection', true)\n" +
                        "   , ROW ('join_reordering_strategy', 'AUTOMATIC', 'AUTOMATIC', 'varchar', 'Join reordering strategy. Possible values: [NONE, ELIMINATE_CROSS_JOINS, AUTOMATIC]', true)\n" +
                        "   , ROW ('late_materialization', 'false', 'false', 'boolean', 'Experimental: Use late materialization (including WorkProcessor pipelines)', true)\n" +
                        "   , ROW ('max_drivers_per_task', '', '', 'integer', 'Maximum number of drivers per task', true)\n" +
                        "   , ROW ('max_recursion_depth', '10', '10', 'integer', 'Maximum recursion depth for recursive common table expression', true)\n" +
                        "   , ROW ('max_reordered_joins', '9', '9', 'integer', 'The maximum number of joins to reorder as one group in cost-based join reordering', true)\n" +
                        "   , ROW ('max_unacknowledged_splits_per_task', '500', '500', 'integer', 'Maximum number of leaf splits awaiting delivery to a given task', true)\n" +
                        "   , ROW ('merge_project_with_values', 'true', 'true', 'boolean', 'Inline project expressions into values', true)\n" +
                        "   , ROW ('omit_datetime_type_precision', 'false', 'false', 'boolean', 'Omit precision when rendering datetime type names with default precision', true)\n" +
                        "   , ROW ('optimize_duplicate_insensitive_joins', 'true', 'true', 'boolean', 'Optimize duplicate insensitive joins', true)\n" +
                        "   , ROW ('optimize_hash_generation', 'true', 'true', 'boolean', 'Compute hash codes for distribution, joins, and aggregations early in query plan', true)\n" +
                        "   , ROW ('optimize_metadata_queries', 'false', 'false', 'boolean', 'Enable optimization for metadata queries', true)\n" +
                        "   , ROW ('optimize_mixed_distinct_aggregations', 'false', 'false', 'boolean', 'Optimize mixed non-distinct and distinct aggregations', true)\n" +
                        "   , ROW ('optimize_top_n_ranking', 'true', 'true', 'boolean', 'Use top N ranking optimization', true)\n" +
                        "   , ROW ('parse_decimal_literals_as_double', 'false', 'false', 'boolean', 'Parse decimal literals as DOUBLE instead of DECIMAL', true)\n" +
                        "   , ROW ('predicate_pushdown_use_table_properties', 'true', 'true', 'boolean', 'Use table properties in predicate pushdown', true)\n" +
                        "   , ROW ('prefer_partial_aggregation', 'true', 'true', 'boolean', 'Prefer splitting aggregations into partial and final stages', true)\n" +
                        "   , ROW ('prefer_streaming_operators', 'false', 'false', 'boolean', 'Prefer source table layouts that produce streaming operators', true)\n" +
                        "   , ROW ('preferred_write_partitioning_min_number_of_partitions', '50', '50', 'integer', 'Use preferred write partitioning when the number of written partitions exceeds the configured threshold', true)\n" +
                        "   , ROW ('push_aggregation_through_outer_join', 'true', 'true', 'boolean', 'Allow pushing aggregations below joins', true)\n" +
                        "   , ROW ('push_partial_aggregation_through_join', 'false', 'false', 'boolean', 'Push partial aggregations below joins', true)\n" +
                        "   , ROW ('push_table_write_through_union', 'true', 'true', 'boolean', 'Parallelize writes when using UNION ALL in queries that write data', true)\n" +
                        "   , ROW ('query_max_cpu_time', '1000000000.00d', '1000000000.00d', 'varchar', 'Maximum CPU time of a query', true)\n" +
                        "   , ROW ('query_max_execution_time', '100.00d', '100.00d', 'varchar', 'Maximum execution time of a query', true)\n" +
                        "   , ROW ('query_max_planning_time', '10.00m', '10.00m', 'varchar', 'Maximum planning time of a query', true)\n" +
                        "   , ROW ('query_max_run_time', '100.00d', '100.00d', 'varchar', 'Maximum run time of a query (includes the queueing time)', true)\n" +
                        "   , ROW ('query_max_scan_physical_bytes', '', '', 'varchar', 'Maximum scan physical bytes of a query', true)\n" +
                        "   , ROW ('query_priority', '1', '1', 'integer', 'The priority of queries. Larger numbers are higher priority', true)\n" +
                        "   , ROW ('redistribute_writes', 'true', 'true', 'boolean', 'Force parallel distributed writes', true)\n" +
                        "   , ROW ('required_workers_count', '1', '1', 'integer', 'Minimum number of active workers that must be available before the query will start', true)\n" +
                        "   , ROW ('required_workers_max_wait_time', '5.00m', '5.00m', 'varchar', 'Maximum time to wait for minimum number of workers before the query is failed', true)\n" +
                        "   , ROW ('resource_overcommit', 'false', 'false', 'boolean', 'Use resources which are not guaranteed to be available to the query', true)\n" +
                        "   , ROW ('rewrite_filtering_semi_join_to_inner_join', 'true', 'true', 'boolean', 'Rewrite semi join in filtering context to inner join', true)\n" +
                        "   , ROW ('scale_writers', 'false', 'false', 'boolean', 'Scale out writers based on throughput (use minimum necessary)', true)\n" +
                        "   , ROW ('skip_redundant_sort', 'true', 'true', 'boolean', 'Skip redundant sort operations', true)\n" +
                        "   , ROW ('spatial_join', 'true', 'true', 'boolean', 'Use spatial index for spatial join when possible', true)\n" +
                        "   , ROW ('spatial_partitioning_table_name', '', '', 'varchar', 'Name of the table containing spatial partitioning scheme', true)\n" +
                        "   , ROW ('spill_enabled', 'false', 'false', 'boolean', 'Enable spilling', true)\n" +
                        "   , ROW ('spill_order_by', 'true', 'true', 'boolean', 'Spill in OrderBy if spill_enabled is also set', true)\n" +
                        "   , ROW ('spill_window_operator', 'true', 'true', 'boolean', 'Spill in WindowOperator if spill_enabled is also set', true)\n" +
                        "   , ROW ('split_concurrency_adjustment_interval', '100.00ms', '100.00ms', 'varchar', 'Experimental: Interval between changes to the number of concurrent splits per node', true)\n" +
                        "   , ROW ('statistics_cpu_timer_enabled', 'true', 'true', 'boolean', 'Experimental: Enable cpu time tracking for automatic column statistics collection on write', true)\n" +
                        "   , ROW ('statistics_precalculation_for_pushdown_enabled', 'false', 'false', 'boolean', 'Enable statistics precalculation for pushdown', true)\n" +
                        "   , ROW ('table_scan_node_partitioning_min_bucket_to_task_ratio', '0.5', '0.5', 'double', 'Min table scan bucket to task ratio for which plan will be adopted to node pre-partitioned tables', true)\n" +
                        "   , ROW ('task_concurrency', '16', '16', 'integer', 'Default number of local parallel jobs per worker', true)\n" +
                        "   , ROW ('task_share_index_loading', 'false', 'false', 'boolean', 'Share index join lookups and caching within a task', true)\n" +
                        "   , ROW ('task_writer_count', '1', '1', 'integer', 'Default number of local parallel table writer jobs per worker', true)\n" +
                        "   , ROW ('unwrap_casts', 'true', 'true', 'boolean', 'Enable optimization to unwrap CAST expression', true)\n" +
                        "   , ROW ('use_legacy_window_filter_pushdown', 'false', 'false', 'boolean', 'Use legacy window filter pushdown optimizer', true)\n" +
                        "   , ROW ('use_mark_distinct', 'true', 'true', 'boolean', 'Implement DISTINCT aggregations using MarkDistinct', true)\n" +
                        "   , ROW ('use_preferred_write_partitioning', 'true', 'true', 'boolean', 'Use preferred write partitioning', true)\n" +
                        "   , ROW ('use_table_scan_node_partitioning', 'true', 'true', 'boolean', 'Adapt plan to node pre-partitioned tables', true)\n" +
                        "   , ROW ('writer_min_size', '32MB', '32MB', 'varchar', 'Target minimum size of writer output when scaling writers', true)\n" +
                        "   , ROW ('', '', '', '', '', false)\n" +
                        ")  \"session\" (\"name\", \"value\", \"default\", \"type\", \"description\", \"include\")\n" +
                        "WHERE (include AND (name LIKE '%' ESCAPE '$'))\n");
    }

    private void assertFormatSql(Statement node, Rewrite rewriteProvider, String expected)
    {
        transaction(transactionManager, accessControl)
                .readUncommitted()
                .execute(beginTransaction(CLIENT_SESSION_BUILDER), session -> {
                    Statement rewrittenNode = rewrite(session, node, rewriteProvider);
                    String actual = SqlFormatterUtil.getFormattedSql(rewrittenNode, SQL_PARSER);
                    assertThat(actual).isEqualTo(expected);
                });
    }

    private Statement rewrite(Session session, Statement node, Rewrite rewriteProvider)
    {
        return rewriteProvider.rewrite(
                session,
                metadata,
                SQL_PARSER,
                Optional.empty(),
                node,
                emptyList(),
                emptyMap(),
                user -> ImmutableSet.of(),
                accessControl,
                NOOP,
                noopStatsCalculator());
    }

    private Session beginTransaction(Session.SessionBuilder sessionBuilder)
    {
        return sessionBuilder.setTransactionId(transactionManager.beginTransaction(false)).build();
    }

    @BeforeClass
    public void setup()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig());
        accessControl.loadSystemAccessControl();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        metadata.addFunctions(ImmutableList.of(APPLY_FUNCTION));

        Catalog tpchTestCatalog = createTestingCatalog(TPCH_CATALOG, TPCH_CATALOG_NAME);
        catalogManager.registerCatalog(tpchTestCatalog);
        metadata.getTablePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getTableProperties());
        metadata.getAnalyzePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getAnalyzeProperties());

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table1, ImmutableList.of(new ColumnMetadata("a", BIGINT))),
                false));
    }

    private Catalog createTestingCatalog(String catalogName, CatalogName catalog)
    {
        CatalogName systemId = createSystemTablesCatalogName(catalog);
        Connector connector = createTestingConnector();
        metadata.getSessionPropertyManager().addConnectorSessionProperties(new CatalogName(catalogName), connector.getSessionProperties());
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalog)));
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    private static Connector createTestingConnector()
    {
        return new Connector()
        {
            private final ConnectorMetadata metadata = new TestingMetadata();

            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return metadata;
            }

            @Override
            public List<PropertyMetadata<?>> getAnalyzeProperties()
            {
                return ImmutableList.of();
            }
        };
    }
}
