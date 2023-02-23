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
package io.trino.sql.query;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionBundle;
import io.trino.spi.Plugin;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.ListAssert;
import org.assertj.core.presentation.Representation;
import org.assertj.core.presentation.StandardRepresentation;
import org.assertj.core.util.CanIgnoreReturnValue;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.sql.planner.assertions.PlanAssert.assertPlan;
import static io.trino.sql.query.QueryAssertions.QueryAssert.newQueryAssert;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class QueryAssertions
        implements Closeable
{
    private final QueryRunner runner;

    public QueryAssertions()
    {
        this(testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("default")
                .build());
    }

    public QueryAssertions(Session session)
    {
        this(LocalQueryRunner.create(session));
    }

    public QueryAssertions(QueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
    }

    public Session.SessionBuilder sessionBuilder()
    {
        return Session.builder(runner.getDefaultSession());
    }

    public Session getDefaultSession()
    {
        return runner.getDefaultSession();
    }

    public void addFunctions(FunctionBundle functionBundle)
    {
        runner.addFunctions(functionBundle);
    }

    public void addPlugin(Plugin plugin)
    {
        runner.installPlugin(plugin);
    }

    public AssertProvider<QueryAssert> query(@Language("SQL") String query)
    {
        return query(runner.getDefaultSession(), query);
    }

    public AssertProvider<QueryAssert> query(Session session, @Language("SQL") String query)
    {
        return newQueryAssert(query, runner, session);
    }

    public ExpressionAssertProvider expression(@Language("SQL") String expression)
    {
        return expression(expression, runner.getDefaultSession());
    }

    public ExpressionAssertProvider operator(OperatorType operator, @Language("SQL") String... arguments)
    {
        return function(mangleOperatorName(operator), arguments);
    }

    public ExpressionAssertProvider function(String name, List<String> arguments)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (int i = 0; i < arguments.size(); i++) {
            builder.add("a" + i);
        }

        List<String> names = builder.build();
        ExpressionAssertProvider assertion = expression("\"%s\"(%s)".formatted(
                name,
                String.join(",", names)));

        for (int i = 0; i < arguments.size(); i++) {
            assertion.binding(names.get(i), arguments.get(i));
        }

        return assertion;
    }

    public ExpressionAssertProvider function(String name, @Language("SQL") String... arguments)
    {
        return function(name, Arrays.asList(arguments));
    }

    public ExpressionAssertProvider expression(@Language("SQL") String expression, Session session)
    {
        return new ExpressionAssertProvider(runner, session, expression);
    }

    public void assertQueryAndPlan(
            @Language("SQL") String actual,
            @Language("SQL") String expected,
            PlanMatchPattern pattern)
    {
        assertQuery(runner.getDefaultSession(), actual, expected, false);

        Plan plan = runner.executeWithPlan(runner.getDefaultSession(), actual, WarningCollector.NOOP).getQueryPlan();
        PlanAssert.assertPlan(runner.getDefaultSession(), runner.getMetadata(), runner.getFunctionManager(), runner.getStatsCalculator(), plan, pattern);
    }

    private void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
    {
        MaterializedResult actualResults = null;
        try {
            actualResults = execute(session, actual);
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }

        MaterializedResult expectedResults = null;
        try {
            expectedResults = execute(expected);
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }

        assertEquals(expectedResults.getTypes(), actualResults.getTypes(), "Types mismatch for query: \n " + actual + "\n:");

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        if (ensureOrdering) {
            if (!actualRows.equals(expectedRows)) {
                assertEquals(expectedRows, actualRows, "For query: \n " + actual + "\n:");
            }
        }
        else {
            assertEqualsIgnoreOrder(actualRows, expectedRows, "For query: \n " + actual);
        }
    }

    public void assertQueryReturnsEmptyResult(@Language("SQL") String actual)
    {
        MaterializedResult actualResults = null;
        try {
            actualResults = execute(actual);
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }
        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        assertEquals(0, actualRows.size());
    }

    public MaterializedResult execute(@Language("SQL") String query)
    {
        return execute(runner.getDefaultSession(), query);
    }

    public MaterializedResult execute(Session session, @Language("SQL") String query)
    {
        MaterializedResult actualResults;
        actualResults = runner.execute(session, query).toTestTypes();
        return actualResults;
    }

    @Override
    public void close()
    {
        runner.close();
    }

    public QueryRunner getQueryRunner()
    {
        return runner;
    }

    protected void executeExclusively(Runnable executionBlock)
    {
        runner.getExclusiveLock().lock();
        try {
            executionBlock.run();
        }
        finally {
            runner.getExclusiveLock().unlock();
        }
    }

    public static class QueryAssert
            extends AbstractAssert<QueryAssert, MaterializedResult>
    {
        private static final Representation ROWS_REPRESENTATION = new StandardRepresentation()
        {
            @Override
            public String toStringOf(Object object)
            {
                if (object instanceof List<?> list) {
                    return list.stream()
                            .map(this::toStringOf)
                            .collect(Collectors.joining(", "));
                }
                if (object instanceof MaterializedRow row) {
                    return row.getFields().stream()
                            .map(this::formatRowElement)
                            .collect(Collectors.joining(", ", "(", ")"));
                }
                return super.toStringOf(object);
            }

            private String formatRowElement(Object value)
            {
                if (value == null) {
                    return "null";
                }
                if (value.getClass().isArray()) {
                    return formatArray(value);
                }
                // Using super.toStringOf would add quotes around String values, which could be expected for varchar values
                // but would be misleading for date/time values which come as String too. More proper formatting would need to be
                // type-aware.
                return String.valueOf(value);
            }
        };

        private final QueryRunner runner;
        private final Session session;
        private final String query;
        private boolean ordered;
        private boolean skipTypesCheck;
        private boolean skipResultsCorrectnessCheckForPushdown;

        static AssertProvider<QueryAssert> newQueryAssert(String query, QueryRunner runner, Session session)
        {
            MaterializedResult result = runner.execute(session, query);
            return () -> new QueryAssert(runner, session, query, result, false, false, false);
        }

        private QueryAssert(
                QueryRunner runner,
                Session session,
                String query,
                MaterializedResult actual,
                boolean ordered,
                boolean skipTypesCheck,
                boolean skipResultsCorrectnessCheckForPushdown)
        {
            super(actual, Object.class);
            this.runner = requireNonNull(runner, "runner is null");
            this.session = requireNonNull(session, "session is null");
            this.query = requireNonNull(query, "query is null");
            this.ordered = ordered;
            this.skipTypesCheck = skipTypesCheck;
            this.skipResultsCorrectnessCheckForPushdown = skipResultsCorrectnessCheckForPushdown;
        }

        // TODO for better readability, replace this with `exceptColumns(String... columnNamesToExclude)` leveraging MaterializedResult.getColumnNames
        @Deprecated
        public QueryAssert projected(int... columns)
        {
            return new QueryAssert(
                    runner,
                    session,
                    format("%s projected with %s", query, Arrays.toString(columns)),
                    new MaterializedResult(
                            actual.getMaterializedRows().stream()
                                    .map(row -> new MaterializedRow(
                                            row.getPrecision(),
                                            IntStream.of(columns)
                                                    .mapToObj(row::getField)
                                                    .collect(toList()))) // values are nullable
                                    .collect(toImmutableList()),
                            IntStream.of(columns)
                                    .mapToObj(actual.getTypes()::get)
                                    .collect(toImmutableList())),
                    ordered,
                    skipTypesCheck,
                    skipResultsCorrectnessCheckForPushdown);
        }

        public QueryAssert matches(BiFunction<Session, QueryRunner, MaterializedResult> evaluator)
        {
            MaterializedResult expected = evaluator.apply(session, runner);
            return matches(expected);
        }

        public QueryAssert ordered()
        {
            ordered = true;
            return this;
        }

        public QueryAssert skippingTypesCheck()
        {
            skipTypesCheck = true;
            return this;
        }

        public QueryAssert skipResultsCorrectnessCheckForPushdown()
        {
            skipResultsCorrectnessCheckForPushdown = true;
            return this;
        }

        @CanIgnoreReturnValue
        public QueryAssert matches(@Language("SQL") String query)
        {
            MaterializedResult expected = runner.execute(session, query);
            return matches(expected);
        }

        @CanIgnoreReturnValue
        public QueryAssert matches(MaterializedResult expected)
        {
            return satisfies(actual -> {
                if (!skipTypesCheck) {
                    assertTypes(query, actual, expected.getTypes());
                }

                ListAssert<MaterializedRow> assertion = assertThat(actual.getMaterializedRows())
                        .as("Rows for query [%s]", query)
                        .withRepresentation(ROWS_REPRESENTATION);

                if (ordered) {
                    assertion.containsExactlyElementsOf(expected.getMaterializedRows());
                }
                else {
                    assertion.containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
                }
            });
        }

        @CanIgnoreReturnValue
        public QueryAssert matches(PlanMatchPattern expectedPlan)
        {
            transaction(runner.getTransactionManager(), runner.getAccessControl())
                    .execute(session, session -> {
                        Plan plan = runner.createPlan(session, query, WarningCollector.NOOP);
                        assertPlan(
                                session,
                                runner.getMetadata(),
                                runner.getFunctionManager(),
                                noopStatsCalculator(),
                                plan,
                                expectedPlan);
                    });
            return this;
        }

        @CanIgnoreReturnValue
        public QueryAssert containsAll(@Language("SQL") String query)
        {
            MaterializedResult expected = runner.execute(session, query);
            return containsAll(expected);
        }

        @CanIgnoreReturnValue
        public QueryAssert containsAll(MaterializedResult expected)
        {
            return satisfies(actual -> {
                if (!skipTypesCheck) {
                    assertTypes(query, actual, expected.getTypes());
                }

                assertThat(actual.getMaterializedRows())
                        .as("Rows for query [%s]", query)
                        .withRepresentation(ROWS_REPRESENTATION)
                        .containsAll(expected.getMaterializedRows());
            });
        }

        @CanIgnoreReturnValue
        public QueryAssert hasColumnNames(String... expectedColumnNames)
        {
            return satisfies(actual -> {
                assertThat(actual.getColumnNames())
                        .as("Column names for query [%s]", query)
                        .containsExactly(expectedColumnNames);
            });
        }

        @CanIgnoreReturnValue
        public QueryAssert hasOutputTypes(List<Type> expectedTypes)
        {
            return satisfies(actual -> {
                assertTypes(query, actual, expectedTypes);
            });
        }

        @CanIgnoreReturnValue
        public QueryAssert outputHasType(int index, Type expectedType)
        {
            return satisfies(actual -> {
                assertThat(actual.getTypes())
                        .as("Output types for query [%s]", query)
                        .element(index).isEqualTo(expectedType);
            });
        }

        private static void assertTypes(String query, MaterializedResult actual, List<Type> expectedTypes)
        {
            assertThat(actual.getTypes())
                    .as("Output types for query [%s]", query)
                    .isEqualTo(expectedTypes);
        }

        @CanIgnoreReturnValue
        public QueryAssert returnsEmptyResult()
        {
            return satisfies(actual -> {
                assertThat(actual.getMaterializedRows()).as("Rows for query [%s]", query).isEmpty();
            });
        }

        /**
         * Verifies query is fully pushed down and that results are the same as when pushdown is fully disabled.
         */
        @CanIgnoreReturnValue
        public QueryAssert isFullyPushedDown()
        {
            checkState(!(runner instanceof LocalQueryRunner), "isFullyPushedDown() currently does not work with LocalQueryRunner");

            transaction(runner.getTransactionManager(), runner.getAccessControl())
                    .execute(session, session -> {
                        Plan plan = runner.createPlan(session, query, WarningCollector.NOOP);
                        assertPlan(
                                session,
                                runner.getMetadata(),
                                runner.getFunctionManager(),
                                noopStatsCalculator(),
                                plan,
                                PlanMatchPattern.output(PlanMatchPattern.node(TableScanNode.class)));
                    });

            if (!skipResultsCorrectnessCheckForPushdown) {
                // Compare the results with pushdown disabled, so that explicit matches() call is not needed
                hasCorrectResultsRegardlessOfPushdown();
            }
            return this;
        }

        /**
         * Verifies query is not fully pushed down and that results are the same as when pushdown is fully disabled.
         * <p>
         * <b>Note:</b> the primary intent of this assertion is to ensure the test is updated to {@link #isFullyPushedDown()}
         * when pushdown capabilities are improved.
         */
        @SafeVarargs
        @CanIgnoreReturnValue
        public final QueryAssert isNotFullyPushedDown(Class<? extends PlanNode> firstRetainedNode, Class<? extends PlanNode>... moreRetainedNodes)
        {
            PlanMatchPattern expectedPlan = PlanMatchPattern.node(TableScanNode.class);
            List<Class<? extends PlanNode>> retainedNodes = Lists.asList(firstRetainedNode, moreRetainedNodes);
            for (Class<? extends PlanNode> retainedNode : ImmutableList.copyOf(retainedNodes).reverse()) {
                expectedPlan = PlanMatchPattern.node(retainedNode, expectedPlan);
            }
            return isNotFullyPushedDown(expectedPlan);
        }

        /**
         * Verifies query is not fully pushed down and that results are the same as when pushdown is fully disabled.
         * <p>
         * <b>Note:</b> the primary intent of this assertion is to ensure the test is updated to {@link #isFullyPushedDown()}
         * when pushdown capabilities are improved.
         */
        @CanIgnoreReturnValue
        public QueryAssert isNotFullyPushedDown(PlanMatchPattern retainedSubplan)
        {
            PlanMatchPattern expectedPlan = PlanMatchPattern.anyTree(retainedSubplan);

            return hasPlan(expectedPlan, plan -> {
                if (PlanNodeSearcher.searchFrom(plan.getRoot())
                        .whereIsInstanceOfAny(TableScanNode.class)
                        .findFirst().isEmpty()) {
                    throw new IllegalArgumentException("Incorrect use of isNotFullyPushedDown: the actual plan matched the expected despite not having a TableScanNode left " +
                            "in the plan. Use hasPlan() instead");
                }
            });
        }

        /**
         * Verifies join query is not fully pushed down by containing JOIN node.
         */
        @CanIgnoreReturnValue
        public QueryAssert joinIsNotFullyPushedDown()
        {
            return verifyPlan(plan -> {
                if (PlanNodeSearcher.searchFrom(plan.getRoot())
                        .whereIsInstanceOfAny(JoinNode.class)
                        .findFirst()
                        .isEmpty()) {
                    throw new IllegalStateException("Join node should be present in explain plan, when pushdown is not applied");
                }
            });
        }

        /**
         * Verifies query has the expected plan and that results are the same as when pushdown is fully disabled.
         */
        @CanIgnoreReturnValue
        public QueryAssert hasPlan(PlanMatchPattern expectedPlan)
        {
            return hasPlan(expectedPlan, plan -> {});
        }

        private QueryAssert hasPlan(PlanMatchPattern expectedPlan, Consumer<Plan> additionalPlanVerification)
        {
            transaction(runner.getTransactionManager(), runner.getAccessControl())
                    .execute(session, session -> {
                        Plan plan = runner.createPlan(session, query, WarningCollector.NOOP);
                        assertPlan(
                                session,
                                runner.getMetadata(),
                                runner.getFunctionManager(),
                                noopStatsCalculator(),
                                plan,
                                expectedPlan);
                        additionalPlanVerification.accept(plan);
                    });

            if (!skipResultsCorrectnessCheckForPushdown) {
                // Compare the results with pushdown disabled, so that explicit matches() call is not needed
                hasCorrectResultsRegardlessOfPushdown();
            }
            return this;
        }

        private QueryAssert verifyPlan(Consumer<Plan> planVerification)
        {
            transaction(runner.getTransactionManager(), runner.getAccessControl())
                    .execute(session, session -> {
                        Plan plan = runner.createPlan(session, query, WarningCollector.NOOP);
                        planVerification.accept(plan);
                    });

            if (!skipResultsCorrectnessCheckForPushdown) {
                // Compare the results with pushdown disabled, so that explicit matches() call is not needed
                hasCorrectResultsRegardlessOfPushdown();
            }
            return this;
        }

        @CanIgnoreReturnValue
        public QueryAssert hasCorrectResultsRegardlessOfPushdown()
        {
            Session withoutPushdown = Session.builder(session)
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            matches(runner.execute(withoutPushdown, query));
            return this;
        }
    }

    public static class ExpressionAssertProvider
            implements AssertProvider<ExpressionAssert>
    {
        private final QueryRunner runner;
        private final String expression;
        private final Session session;

        private final Map<String, String> bindings = new HashMap<>();

        public ExpressionAssertProvider(QueryRunner runner, Session session, String expression)
        {
            this.runner = runner;
            this.session = session;
            this.expression = expression;
        }

        public ExpressionAssertProvider binding(String variable, @Language("SQL") String value)
        {
            String previous = bindings.put(variable, value);
            if (previous != null) {
                fail("%s already bound to: %s".formatted(variable, value));
            }
            return this;
        }

        public Result evaluate()
        {
            if (bindings.isEmpty()) {
                return run("VALUES ROW(%s)".formatted(expression));
            }
            else {
                List<Map.Entry<String, String>> entries = ImmutableList.copyOf(bindings.entrySet());

                List<String> columns = entries.stream()
                        .map(Map.Entry::getKey)
                        .collect(toList());

                List<String> values = entries.stream()
                        .map(Map.Entry::getValue)
                        .collect(toList());

                // Evaluate the expression using two modes:
                //  1. Avoid constant folding -> exercises the compiler and evaluation engine
                //  2. Force constant folding -> exercises the interpreter

                Result full = run("""
                        SELECT %s
                        FROM (
                            VALUES ROW(%s)
                        ) t(%s)
                        WHERE rand() >= 0
                        """
                        .formatted(
                                expression,
                                Joiner.on(",").join(values),
                                Joiner.on(",").join(columns)));

                Result withConstantFolding = run("""
                        SELECT %s
                        FROM (
                            VALUES ROW(%s)
                        ) t(%s)
                        """
                        .formatted(
                                expression,
                                Joiner.on(",").join(values),
                                Joiner.on(",").join(columns)));

                if (!full.type().equals(withConstantFolding.type())) {
                    fail("Mismatched types between interpreter and evaluation engine: %s vs %s".formatted(full.type(), withConstantFolding.type()));
                }

                if (!Objects.equals(full.value(), withConstantFolding.value())) {
                    fail("Mismatched results between interpreter and evaluation engine: %s vs %s".formatted(full.value(), withConstantFolding.value()));
                }

                return new Result(full.type(), full.value);
            }
        }

        private Result run(String query)
        {
            MaterializedResult result = runner.execute(session, query);
            return new Result(result.getTypes().get(0), result.getOnlyColumnAsSet().iterator().next());
        }

        @Override
        public ExpressionAssert assertThat()
        {
            Result result = evaluate();
            return new ExpressionAssert(runner, session, result.value(), result.type())
                    .withRepresentation(ExpressionAssert.TYPE_RENDERER);
        }

        record Result(Type type, Object value) {}
    }

    public static class ExpressionAssert
            extends AbstractAssert<ExpressionAssert, Object>
    {
        private static final StandardRepresentation TYPE_RENDERER = new StandardRepresentation()
        {
            @Override
            public String toStringOf(Object object)
            {
                if (object instanceof SqlTimestamp timestamp) {
                    return String.format(
                            "%s [p = %s, epochMicros = %s, fraction = %s]",
                            timestamp,
                            timestamp.getPrecision(),
                            timestamp.getEpochMicros(),
                            timestamp.getPicosOfMicros());
                }
                if (object instanceof SqlTimestampWithTimeZone timestamp) {
                    return String.format(
                            "%s [p = %s, epochMillis = %s, fraction = %s, tz = %s]",
                            timestamp,
                            timestamp.getPrecision(),
                            timestamp.getEpochMillis(),
                            timestamp.getPicosOfMilli(),
                            timestamp.getTimeZoneKey());
                }
                if (object instanceof SqlTime time) {
                    return String.format("%s [picos = %s]", time, time.getPicos());
                }
                if (object instanceof SqlTimeWithTimeZone time) {
                    return String.format(
                            "%s [picos = %s, offset = %s]",
                            time,
                            time.getPicos(),
                            time.getOffsetMinutes());
                }

                return Objects.toString(object);
            }
        };

        private final QueryRunner runner;
        private final Session session;
        private final Type actualType;

        public ExpressionAssert(QueryRunner runner, Session session, Object actual, Type actualType)
        {
            super(actual, Object.class);
            this.runner = runner;
            this.session = session;
            this.actualType = actualType;
        }

        public ExpressionAssert isEqualTo(BiFunction<Session, QueryRunner, Object> evaluator)
        {
            return isEqualTo(evaluator.apply(session, runner));
        }

        public ExpressionAssert matches(@Language("SQL") String expression)
        {
            MaterializedResult result = runner.execute(session, "VALUES " + expression);
            Type expectedType = result.getTypes().get(0);
            Object expectedValue = result.getOnlyColumnAsSet().iterator().next();

            return satisfies(actual -> {
                assertThat(actualType).as("Type")
                        .isEqualTo(expectedType);

                assertThat(actual)
                        .withRepresentation(TYPE_RENDERER)
                        .isEqualTo(expectedValue);
            });
        }

        /**
         * Syntactic sugar for:
         *
         * <pre>{@code
         *     assertThat(...)
         *         .hasType(type)
         *         .isNull()
         * }</pre>
         */
        public void isNull(Type type)
        {
            hasType(type).isNull();
        }

        public ExpressionAssert hasType(Type type)
        {
            objects.assertEqual(info, actualType, type);
            return this;
        }
    }
}
