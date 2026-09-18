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
package io.trino.operator.window;

import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class AbstractTestWindowFunction
{
    protected QueryRunner queryRunner;

    @BeforeAll
    public final void initTestWindowFunction()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
    }

    @AfterAll
    public final void destroyTestWindowFunction()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    /// The window function exercised by {@link #testNonNullResultHoldsWithNullInput}: its name (for
    /// resolving the declared result nullability) and an invocation without the OVER clause, e.g.
    /// {@code new WindowFunctionUnderTest("ntile", "ntile(4)")}. Overridden by subclasses whose
    /// function declares a non-null result; the default skips the check.
    protected Optional<WindowFunctionUnderTest> windowFunctionUnderTest()
    {
        return Optional.empty();
    }

    protected record WindowFunctionUnderTest(String name, @Language("SQL") String invocation) {}

    @Test
    public void testNonNullResultHoldsWithNullInput()
    {
        Optional<WindowFunctionUnderTest> function = windowFunctionUnderTest();
        if (function.isEmpty()) {
            return;
        }
        boolean nullable = queryRunner.inTransaction(session -> queryRunner.getPlannerContext().getMetadata()
                .getFunctions(session, builtinFunctionName(function.get().name())).stream()
                .anyMatch(candidate -> candidate.functionMetadata().getFunctionNullability().isReturnNullable()));
        if (nullable) {
            // A nullable result declaration is always sound; only a non-null declaration must be verified.
            return;
        }
        // A function that declares a non-null result must not produce NULL, even over input with nulls.
        MaterializedResult result = executeWindowQueryWithNulls(function.get().invocation() + " OVER (ORDER BY orderkey)");
        assertThat(result.getMaterializedRows())
                .as("%s produced no rows; nullability cannot be verified", function.get().name())
                .isNotEmpty();
        for (MaterializedRow row : result.getMaterializedRows()) {
            assertThat(row.getField(row.getFieldCount() - 1))
                    .describedAs("%s declares a non-null result but produced NULL over input containing nulls", function.get().name())
                    .isNotNull();
        }
    }

    protected void assertWindowQuery(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQuery(sql, expected, queryRunner);
    }

    protected void assertUnboundedWindowQuery(@Language("SQL") String sql, MaterializedResult expected)
    {
        assertWindowQuery(unbounded(sql), expected);
    }

    protected void assertWindowQueryWithNulls(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQueryWithNulls(sql, expected, queryRunner);
    }

    protected MaterializedResult executeWindowQueryWithNulls(@Language("SQL") String sql)
    {
        return WindowAssertions.executeWindowQueryWithNulls(sql, queryRunner);
    }

    protected void assertWindowQueryWithNan(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQueryWithNan(sql, expected, queryRunner);
    }

    protected void assertWindowQueryWithInfinity(@Language("SQL") String sql, MaterializedResult expected)
    {
        WindowAssertions.assertWindowQueryWithInfinity(sql, expected, queryRunner);
    }

    protected void assertUnboundedWindowQueryWithNulls(@Language("SQL") String sql, MaterializedResult expected)
    {
        assertWindowQueryWithNulls(unbounded(sql), expected);
    }

    @Language("SQL")
    private static String unbounded(@Language("SQL") String sql)
    {
        checkArgument(sql.endsWith(")"), "SQL does not end with ')'");
        return sql.substring(0, sql.length() - 1) + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
    }
}
