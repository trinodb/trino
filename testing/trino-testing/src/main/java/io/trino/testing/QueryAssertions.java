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
package io.trino.testing;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.FailureException;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.planner.Plan;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertThatTrinoException;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public final class QueryAssertions
{
    private static final Logger log = Logger.get(QueryAssertions.class);

    private QueryAssertions() {}

    public static void assertUpdate(QueryRunner queryRunner, Session session, @Language("SQL") String sql, OptionalLong count, Optional<Consumer<Plan>> planAssertion)
    {
        if (queryRunner instanceof DistributedQueryRunner distributedQueryRunner) {
            assertDistributedUpdate(distributedQueryRunner, session, sql, count, planAssertion);
            return;
        }

        long start = System.nanoTime();
        MaterializedResult results;
        Plan queryPlan;
        if (planAssertion.isPresent()) {
            MaterializedResultWithPlan resultWithPlan = queryRunner.executeWithPlan(session, sql);
            queryPlan = resultWithPlan.queryPlan().orElseThrow();
            results = resultWithPlan.result().toTestTypes();
        }
        else {
            queryPlan = null;
            results = queryRunner.execute(session, sql);
        }
        Duration queryTime = nanosSince(start);
        if (queryTime.compareTo(Duration.succinctDuration(1, SECONDS)) > 0) {
            log.debug("FINISHED in Trino: %s", queryTime);
        }

        if (planAssertion.isPresent()) {
            planAssertion.get().accept(queryPlan);
        }

        if (results.getUpdateType().isEmpty()) {
            fail("update type is not set");
        }

        if (results.getUpdateCount().isPresent()) {
            if (count.isEmpty()) {
                fail("expected no update count, but got " + results.getUpdateCount().getAsLong());
            }
            assertThat(results.getUpdateCount().getAsLong())
                    .describedAs("update count")
                    .isEqualTo(count.getAsLong());
        }
        else if (count.isPresent()) {
            fail("update count is not present");
        }
    }

    private static void assertDistributedUpdate(DistributedQueryRunner distributedQueryRunner, Session session, @Language("SQL") String sql, OptionalLong count, Optional<Consumer<Plan>> planAssertion)
    {
        long start = System.nanoTime();
        Plan queryPlan = null;
        MaterializedResultWithPlan resultWithPlan = distributedQueryRunner.executeWithPlan(session, sql);
        QueryId queryId = resultWithPlan.queryId();
        MaterializedResult results = resultWithPlan.result().toTestTypes();
        if (planAssertion.isPresent()) {
            try {
                queryPlan = distributedQueryRunner.getQueryPlan(queryId);
            }
            catch (RuntimeException e) {
                fail("Failed to get query plan for query " + queryId, e);
            }
        }

        Duration queryTime = nanosSince(start);
        if (queryTime.compareTo(Duration.succinctDuration(1, SECONDS)) > 0) {
            log.debug("FINISHED query %s in Trino: %s", queryId, queryTime);
        }

        if (planAssertion.isPresent()) {
            try {
                planAssertion.get().accept(queryPlan);
            }
            catch (Exception e) {
                fail("Plan assertion failed for query " + queryId, e);
            }
        }

        if (results.getUpdateType().isEmpty()) {
            fail("update type is not set for query " + queryId);
        }

        if (results.getUpdateCount().isPresent()) {
            if (count.isEmpty()) {
                fail("expected no update count, but got " + results.getUpdateCount().getAsLong() + " for query " + queryId);
            }
            assertThat(results.getUpdateCount().getAsLong())
                    .describedAs("update count for query " + queryId)
                    .isEqualTo(count.getAsLong());
        }
        else if (count.isPresent()) {
            fail("update count is not present for query " + queryId);
        }
    }

    public static void assertQuery(
            QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate)
    {
        assertQuery(actualQueryRunner, session, actual, h2QueryRunner, expected, ensureOrdering, compareUpdate, Optional.empty());
    }

    public static void assertQuery(
            QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate,
            Consumer<Plan> planAssertion)
    {
        assertQuery(actualQueryRunner, session, actual, h2QueryRunner, expected, ensureOrdering, compareUpdate, Optional.of(planAssertion));
    }

    private static void assertQuery(
            QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate,
            Optional<Consumer<Plan>> planAssertion)
    {
        if (actualQueryRunner instanceof DistributedQueryRunner distributedQueryRunner) {
            assertDistributedQuery(distributedQueryRunner, session, actual, h2QueryRunner, expected, ensureOrdering, compareUpdate, planAssertion);
            return;
        }

        long start = System.nanoTime();
        MaterializedResult actualResults = null;
        Plan queryPlan = null;
        if (planAssertion.isPresent()) {
            try {
                MaterializedResultWithPlan resultWithPlan = actualQueryRunner.executeWithPlan(session, actual);
                queryPlan = resultWithPlan.queryPlan().orElseThrow();
                actualResults = resultWithPlan.result().toTestTypes();
            }
            catch (RuntimeException ex) {
                fail("Execution of 'actual' query failed: " + actual, ex);
            }
        }
        else {
            try {
                actualResults = actualQueryRunner.execute(session, actual).toTestTypes();
            }
            catch (RuntimeException ex) {
                fail("Execution of 'actual' query failed: " + actual, ex);
            }
        }
        if (planAssertion.isPresent()) {
            planAssertion.get().accept(queryPlan);
        }
        Duration actualTime = nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = null;
        try {
            expectedResults = h2QueryRunner.execute(session, expected, actualResults.getTypes());
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }
        Duration totalTime = nanosSince(start);
        if (totalTime.compareTo(Duration.succinctDuration(1, SECONDS)) > 0) {
            log.debug("FINISHED in Trino: %s, H2: %s, total: %s", actualTime, nanosSince(expectedStart), totalTime);
        }

        if (actualResults.getUpdateType().isPresent() || actualResults.getUpdateCount().isPresent()) {
            if (actualResults.getUpdateType().isEmpty()) {
                fail("update count present without update type for query: \n" + actual);
            }
            if (!compareUpdate) {
                fail("update type should not be present (use assertUpdate) for query: \n" + actual);
            }
        }

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        if (compareUpdate) {
            if (actualResults.getUpdateType().isEmpty()) {
                fail("update type not present for query: \n" + actual);
            }
            if (actualResults.getUpdateCount().isEmpty()) {
                fail("update count not present for query: \n" + actual);
            }
            assertThat(actualRows.size())
                    .describedAs("For query: \n " + actual + "\n:")
                    .isEqualTo(1);
            assertThat(expectedRows.size())
                    .describedAs("For query: \n " + actual + "\n:")
                    .isEqualTo(1);
            MaterializedRow row = expectedRows.get(0);
            assertThat(row.getFieldCount())
                    .describedAs("For query: \n " + actual + "\n:")
                    .isEqualTo(1);
            assertThat(row.getField(0))
                    .describedAs("For query: \n " + actual + "\n:")
                    .isEqualTo(actualResults.getUpdateCount().getAsLong());
        }

        if (ensureOrdering) {
            if (!actualRows.equals(expectedRows)) {
                assertThat(actualRows)
                        .describedAs("For query: \n " + actual + "\n:")
                        .isEqualTo(expectedRows);
            }
        }
        else {
            assertEqualsIgnoreOrder(actualRows, expectedRows, "For query: \n " + actual);
        }
    }

    private static void assertDistributedQuery(
            DistributedQueryRunner distributedQueryRunner,
            Session session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate,
            Optional<Consumer<Plan>> planAssertion)
    {
        long start = System.nanoTime();
        QueryId queryId = null;
        MaterializedResult actualResults = null;
        try {
            MaterializedResultWithPlan resultWithPlan = distributedQueryRunner.executeWithPlan(session, actual);
            queryId = resultWithPlan.queryId();
            actualResults = resultWithPlan.result().toTestTypes();
        }
        catch (RuntimeException ex) {
            if (queryId == null && ex instanceof QueryFailedException queryFailedException) {
                queryId = queryFailedException.getQueryId();
            }
            if (queryId != null) {
                fail("Execution of 'actual' query " + queryId + " failed: " + actual, ex);
            }
            else {
                fail("Execution of 'actual' query failed: " + actual, ex);
            }
        }
        if (planAssertion.isPresent()) {
            try {
                planAssertion.get().accept(distributedQueryRunner.getQueryPlan(queryId));
            }
            catch (Throwable t) {
                t.addSuppressed(new Exception(format("SQL: %s [QueryId: %s]", actual, queryId)));
                throw t;
            }
        }
        Duration actualTime = nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = null;
        try {
            expectedResults = h2QueryRunner.execute(session, expected, actualResults.getTypes());
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }
        Duration totalTime = nanosSince(start);
        if (totalTime.compareTo(Duration.succinctDuration(1, SECONDS)) > 0) {
            log.debug("FINISHED in Trino: %s, H2: %s, total: %s", actualTime, nanosSince(expectedStart), totalTime);
        }

        if (actualResults.getUpdateType().isPresent() || actualResults.getUpdateCount().isPresent()) {
            if (actualResults.getUpdateType().isEmpty()) {
                fail("update count present without update type for query " + queryId + ": \n" + actual);
            }
            if (!compareUpdate) {
                fail("update type should not be present (use assertUpdate) for query " + queryId + ": \n" + actual);
            }
        }

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        if (compareUpdate) {
            if (actualResults.getUpdateType().isEmpty()) {
                fail("update type not present for query " + queryId + ": \n" + actual);
            }
            if (actualResults.getUpdateCount().isEmpty()) {
                fail("update count not present for query " + queryId + ": \n" + actual);
            }
            assertThat(actualRows.size())
                    .describedAs("For query " + queryId + ": \n " + actual + "\n:")
                    .isEqualTo(1);
            assertThat(expectedRows.size())
                    .describedAs("For query " + queryId + ": \n " + actual + "\n:")
                    .isEqualTo(1);
            MaterializedRow row = expectedRows.get(0);
            assertThat(row.getFieldCount())
                    .describedAs("For query " + queryId + ": \n " + actual + "\n:")
                    .isEqualTo(1);
            assertThat(row.getField(0))
                    .describedAs("For query " + queryId + ": \n " + actual + "\n:")
                    .isEqualTo(actualResults.getUpdateCount().getAsLong());
        }

        if (ensureOrdering) {
            if (!actualRows.equals(expectedRows)) {
                assertThat(actualRows)
                        .describedAs("For query " + queryId + ": \n " + actual + "\n:")
                        .isEqualTo(expectedRows);
            }
        }
        else {
            assertEqualsIgnoreOrder(actualRows, expectedRows, "For query " + queryId + ": \n " + actual);
        }
    }

    public static void assertQueryEventually(
            QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate,
            Optional<Consumer<Plan>> planAssertion,
            Duration timeout)
    {
        assertEventually(timeout, () -> assertQuery(actualQueryRunner, session, actual, h2QueryRunner, expected, ensureOrdering, compareUpdate, planAssertion));
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected)
    {
        assertEqualsIgnoreOrder(actual, expected, null);
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected, String message)
    {
        assertThat(actual)
                .describedAs("actual is null")
                .isNotNull();
        assertThat(expected)
                .describedAs("expected is null")
                .isNotNull();

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actual);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expected);
        if (!actualSet.equals(expectedSet)) {
            Multiset<?> unexpectedRows = Multisets.difference(actualSet, expectedSet);
            Multiset<?> missingRows = Multisets.difference(expectedSet, actualSet);
            int limit = 100;
            fail(format(
                    "%snot equal\n" +
                            "Actual rows (up to %s of %s extra rows shown, %s rows in total):\n    %s\n" +
                            "Expected rows (up to %s of %s missing rows shown, %s rows in total):\n    %s\n",
                    message == null ? "" : (message + "\n"),
                    limit,
                    unexpectedRows.size(),
                    actualSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(unexpectedRows, limit)),
                    limit,
                    missingRows.size(),
                    expectedSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(missingRows, limit))));
        }
    }

    public static void assertContainsEventually(Supplier<MaterializedResult> all, MaterializedResult expectedSubset, Duration timeout)
    {
        assertEventually(timeout, () -> assertContains(all.get(), expectedSubset));
    }

    public static void assertContains(MaterializedResult all, MaterializedResult expectedSubset)
    {
        for (MaterializedRow row : expectedSubset.getMaterializedRows()) {
            if (!all.getMaterializedRows().contains(row)) {
                fail(format("expected row missing: %s\nAll %s rows:\n    %s\nExpected subset %s rows:\n    %s\n",
                        row,
                        all.getMaterializedRows().size(),
                        Joiner.on("\n    ").join(Iterables.limit(all, 100)),
                        expectedSubset.getMaterializedRows().size(),
                        Joiner.on("\n    ").join(Iterables.limit(expectedSubset, 100))));
            }
        }
    }

    protected static void assertQuerySucceeds(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        try {
            queryRunner.execute(session, sql);
        }
        catch (QueryFailedException e) {
            fail(format("Expected query %s to succeed: %s", e.getQueryId(), sql), e);
        }
        catch (RuntimeException e) {
            fail(format("Expected query to succeed: %s", sql), e);
        }
    }

    protected static void assertQueryFailsEventually(QueryRunner queryRunner, Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp, Duration timeout)
    {
        assertEventually(timeout, () -> assertQueryFails(queryRunner, session, sql, expectedMessageRegExp));
    }

    protected static void assertQueryFails(QueryRunner queryRunner, Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        try {
            MaterializedResultWithPlan resultWithPlan = queryRunner.executeWithPlan(session, sql);
            fail(format("Expected query to fail: %s [QueryId: %s]", sql, resultWithPlan.queryId()));
        }
        catch (RuntimeException exception) {
            exception.addSuppressed(new Exception("Query: " + sql));
            assertThatTrinoException(exception)
                    .hasMessageMatching(expectedMessageRegExp);
        }
    }

    protected static void assertQueryReturnsEmptyResult(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        QueryId queryId = null;
        try {
            MaterializedResultWithPlan resultWithPlan = queryRunner.executeWithPlan(session, sql);
            queryId = resultWithPlan.queryId();
            MaterializedResult results = resultWithPlan.result().toTestTypes();
            assertThat(results).isNotNull();
            assertThat(results.getRowCount()).isEqualTo(0);
        }
        catch (RuntimeException ex) {
            if (queryId == null) {
                fail("Execution of query failed: " + sql, ex);
            }
            else {
                fail(format("Execution of query failed: %s [QueryId: %s]", sql, queryId), ex);
            }
        }
    }

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Iterable<TpchTable<?>> tables)
    {
        copyTpchTables(queryRunner, sourceCatalog, sourceSchema, queryRunner.getDefaultSession(), tables);
    }

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), session);
        }
    }

    public static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        copyTable(queryRunner, table, session);
    }

    public static void copyTable(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        long start = System.nanoTime();
        @Language("SQL") String sql = format("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s", table.objectName(), table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.debug("Imported %s rows from %s in %s", rows, table, nanosSince(start));

        assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + table.objectName()).getOnlyValue())
                .as("Table is not loaded properly: %s", table.objectName())
                .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + table).getOnlyValue());
    }

    public static RuntimeException getTrinoExceptionCause(Throwable e)
    {
        return Throwables.getCausalChain(e).stream()
                .filter(QueryAssertions::isTrinoException)
                .findFirst() // TODO .collect(toOptional()) -- should be exactly one in the causal chain
                .map(RuntimeException.class::cast)
                .orElseThrow(() -> new IllegalArgumentException("Exception does not have TrinoException cause", e));
    }

    private static boolean isTrinoException(Throwable exception)
    {
        requireNonNull(exception, "exception is null");

        if (exception instanceof TrinoException || exception instanceof ParsingException) {
            return true;
        }

        if (exception instanceof FailureException failureException) {
            String type = failureException.getFailureInfo().getType();
            return type.equals(TrinoException.class.getName()) || type.equals(ParsingException.class.getName());
        }

        return false;
    }
}
