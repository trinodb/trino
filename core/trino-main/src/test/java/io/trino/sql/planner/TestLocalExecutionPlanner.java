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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.nCopies;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLocalExecutionPlanner
{
    private QueryRunner runner;

    @BeforeAll
    public void setUp()
    {
        runner = new StandaloneQueryRunner(TEST_SESSION);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("tpch", "tpch", ImmutableMap.of());
    }

    @AfterAll
    public void cleanup()
    {
        closeAllRuntimeException(runner);
        runner = null;
    }

    @Test
    public void testProjectionCompilerFailure()
    {
        String inner = "(" + Joiner.on(" + ").join(nCopies(100, "rand()")) + ")";
        String outer = "x + x + " + Joiner.on(" + ").join(nCopies(100, inner));

        assertTrinoExceptionThrownBy(() -> runner.execute("SELECT " + outer + " FROM (VALUES rand()) t(x)"))
                .hasErrorCode(COMPILER_ERROR)
                .hasMessage("Query exceeded maximum columns. Please reduce the number of columns referenced and re-run the query.");
    }

    @Test
    public void testFilterCompilerFailure()
    {
        // Filter Query
        String filterQueryInner = "FROM (SELECT rand() as c1, rand() as c2, rand() as c3)";
        String filterQueryWhere = "WHERE c1 = rand() OR " + Joiner.on(" AND ").join(nCopies(1000, "c1 = rand()"))
                + " OR " + Joiner.on(" AND ").join(nCopies(1000, " c2 = rand()"))
                + " OR " + Joiner.on(" AND ").join(nCopies(1000, " c3 = rand()"));

        assertTrinoExceptionThrownBy(() -> runner.execute("SELECT * " + filterQueryInner + filterQueryWhere))
                .hasErrorCode(COMPILER_ERROR)
                .hasMessage("Query exceeded maximum filters. Please reduce the number of filters referenced and re-run the query.");
    }

    @Test
    public void testExpressionCompilerFailure()
    {
        assertTrinoExceptionThrownBy(() -> runner.execute("SELECT concat(name) FROM nation"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageStartingWith("There must be two or more concatenation arguments");
    }
}
