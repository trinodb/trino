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

package io.trino.sql.planner;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static io.trino.Session.SessionBuilder;
import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseCostBasedPlanTest
        extends BasePlanTest
{
    private static final Logger log = Logger.get(BaseCostBasedPlanTest.class);
    private static final String CATALOG_NAME = "local";

    protected static final List<String> TPCH_SQL_FILES = IntStream.rangeClosed(1, 22)
            .mapToObj(i -> format("q%02d", i))
            .map(queryId -> format("/sql/trino/tpch/%s.sql", queryId))
            .collect(toImmutableList());

    protected static final List<String> TPCDS_SQL_FILES = IntStream.range(1, 100)
            .mapToObj(i -> format("q%02d", i))
            .map(queryId -> format("/sql/trino/tpcds/%s.sql", queryId))
            .collect(toImmutableList());

    private final String schemaName;
    private final boolean partitioned;
    private final IcebergCostBasedPlanTestSetup planTestSetup;

    public BaseCostBasedPlanTest(String schemaName, boolean partitioned)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.partitioned = partitioned;
        this.planTestSetup = new IcebergCostBasedPlanTestSetup();
    }

    @Override
    protected PlanTester createPlanTester()
    {
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(schemaName)
                // Reducing ARM and x86 floating point arithmetic differences, mostly visible at PlanNodeStatsEstimateMath::estimateCorrelatedConjunctionRowCount
                .setSystemProperty("filter_conjunction_independence_factor", "0.750000001")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false");
        PlanTester planTester = PlanTester.create(sessionBuilder.build(), 8);
        planTester.createCatalog(
                CATALOG_NAME,
                planTestSetup.createConnectorFactory(),
                ImmutableMap.of());
        return planTester;
    }

    @BeforeAll
    public void prepareTables()
    {
        planTestSetup.createDatabase(schemaName);
        planTestSetup.populateTablesFromResource(getTableNames(), schemaName, getTableResourceDirectory(), getTableTargetDirectory());
    }

    @AfterAll
    public void cleanUp()
            throws Exception
    {
        planTestSetup.cleanUp();
    }

    protected abstract List<String> getTableNames();

    protected abstract String getTableResourceDirectory();

    protected abstract String getTableTargetDirectory();

    protected abstract List<String> getQueryResourcePaths();

    @ParameterizedTest
    @MethodSource("getQueryResourcePaths")
    public void test(String queryResourcePath)
    {
        assertThat(generateQueryPlan(readQuery(queryResourcePath))).isEqualTo(read(getQueryPlanResourcePath(queryResourcePath)));
    }

    private String getQueryPlanResourcePath(String queryResourcePath)
    {
        Path queryPath = Paths.get(queryResourcePath);
        String connectorName = getPlanTester().getCatalogManager().getCatalog(new CatalogName(CATALOG_NAME)).orElseThrow().getConnectorName().toString();
        Path directory = queryPath.getParent();
        directory = directory.resolve(connectorName);
        directory = directory.resolve(partitioned ? "partitioned" : "unpartitioned");
        String planResourceName = queryPath.getFileName().toString().replaceAll("\\.sql$", ".plan.txt");
        return directory.resolve(planResourceName).toString();
    }

    protected void generate()
    {
        initPlanTest();
        try {
            prepareTables();
            getQueryResourcePaths().stream()
                    .parallel()
                    .forEach(queryResourcePath -> {
                        try {
                            Path queryPlanWritePath = Paths.get(
                                    getSourcePath().toString(),
                                    "src/test/resources",
                                    getQueryPlanResourcePath(queryResourcePath));
                            createParentDirs(queryPlanWritePath.toFile());
                            write(generateQueryPlan(readQuery(queryResourcePath)).getBytes(UTF_8), queryPlanWritePath.toFile());
                            log.info("Generated expected plan for query: %s", queryResourcePath);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            destroyPlanTest();
        }
    }

    private static String readQuery(String resource)
    {
        return read(resource).replaceAll("\\s+;\\s+$", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"")
                .replace("${scale}", "1");
    }

    private static String read(String resource)
    {
        try {
            return Resources.toString(getResource(BaseCostBasedPlanTest.class, resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String generateQueryPlan(String query)
    {
        try {
            return getPlanTester().inTransaction(transactionSession -> {
                PlanTester planTester = getPlanTester();
                Plan plan = planTester.createPlan(transactionSession, query, planTester.getPlanOptimizers(false), OPTIMIZED_AND_VALIDATED, NOOP, createPlanOptimizersStatsCollector());
                JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter(transactionSession);
                plan.getRoot().accept(joinOrderPrinter, 0);
                return joinOrderPrinter.result();
            });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + query, e);
        }
    }

    private Path getSourcePath()
    {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        verify(isDirectory(workingDir), "Working directory is not a directory");
        if (isDirectory(workingDir.resolve(".git"))) {
            // Top-level of the repo
            return workingDir.resolve("testing/trino-tests");
        }
        if (workingDir.getFileName().toString().equals("trino-tests")) {
            return workingDir;
        }
        throw new IllegalStateException("This class must be executed from trino-tests or Trino source directory");
    }

    private class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final Session session;
        private final StringBuilder result = new StringBuilder();

        public JoinOrderPrinter(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        public String result()
        {
            return result.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            JoinNode.DistributionType distributionType = node.getDistributionType()
                    .orElseThrow(() -> new VerifyException("Expected distribution type to be set"));
            if (node.isCrossJoin()) {
                checkState(node.getType() == INNER && distributionType == REPLICATED, "Expected CROSS JOIN to be INNER REPLICATED");
                if (node.isMaySkipOutputDuplicates()) {
                    output(indent, "cross join (can skip output duplicates):");
                }
                else {
                    output(indent, "cross join:");
                }
            }
            else {
                if (node.isMaySkipOutputDuplicates()) {
                    output(indent, "join (%s, %s, can skip output duplicates):", node.getType(), distributionType);
                }
                else {
                    output(indent, "join (%s, %s):", node.getType(), distributionType);
                }
            }

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            Partitioning partitioning = node.getPartitioningScheme().getPartitioning();
            output(
                    indent,
                    "%s exchange (%s, %s, %s)",
                    node.getScope().name().toLowerCase(ENGLISH),
                    node.getType(),
                    partitioning.getHandle(),
                    partitioning.getArguments().stream()
                            .map(BaseCostBasedPlanTest::argumentBindingToString)
                            .sorted() // Currently, order of hash columns is not deterministic
                            .collect(joining(", ", "[", "]")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            output(
                    indent,
                    "%s aggregation over (%s)",
                    node.getStep().name().toLowerCase(ENGLISH),
                    node.getGroupingKeys().stream()
                            .map(Symbol::name)
                            .sorted()
                            .collect(joining(", ")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitFilter(FilterNode node, Integer indent)
        {
            DynamicFilters.ExtractResult filters = extractDynamicFilters(node.getPredicate());
            String inputs = filters.getDynamicConjuncts().stream()
                    .map(descriptor -> ((Reference) descriptor.getInput()).name() + "::" + descriptor.getOperator())
                    .sorted()
                    .collect(joining(", "));

            if (!inputs.isEmpty()) {
                output(indent, "dynamic filter (%s)", inputs);
                indent = indent + 1;
            }
            return visitPlan(node, indent);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            CatalogSchemaTableName tableName = getPlanTester().getPlannerContext().getMetadata().getTableName(session, node.getTable());
            output(indent, "scan %s", tableName.getSchemaTableName().getTableName());

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Integer indent)
        {
            output(indent, "semijoin (%s):", node.getDistributionType().get());

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            output(indent, "values (%s rows)", node.getRowCount());

            return null;
        }

        @FormatMethod
        private void output(int indent, String message, Object... args)
        {
            String formattedMessage = format(message, args);
            result.append(format("%s%s\n", "    ".repeat(indent), formattedMessage));
        }
    }

    private static String argumentBindingToString(Partitioning.ArgumentBinding argument)
    {
        if (argument.isConstant()) {
            return argument.getConstant().getValue().toString();
        }
        return argument.getColumn().name();
    }
}
