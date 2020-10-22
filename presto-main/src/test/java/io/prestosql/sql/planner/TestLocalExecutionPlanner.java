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
package io.prestosql.sql.planner;

import com.google.common.base.Joiner;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.StandardErrorCode.COMPILER_ERROR;
import static io.prestosql.testing.assertions.PrestoExceptionAssert.assertPrestoExceptionThrownBy;
import static java.util.Collections.nCopies;

public class TestLocalExecutionPlanner
{
    private LocalQueryRunner runner;

    @BeforeClass
    public void setUp()
    {
        runner = LocalQueryRunner.create(TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        closeAllRuntimeException(runner);
        runner = null;
    }

    @Test
    public void testProjectionCompilerFailure()
    {
        String inner = "(" + Joiner.on(" + ").join(nCopies(100, "rand()")) + ")";
        String outer = Joiner.on(" + ").join(nCopies(100, inner));

        assertPrestoExceptionThrownBy(() -> runner.execute("SELECT " + outer))
                .hasErrorCode(COMPILER_ERROR)
                .hasMessageStartingWith("Query exceeded maximum columns");
    }

    @Test
    public void testFilterCompilerFailure()
    {
        // Filter Query
        String filterQueryInner = "FROM (SELECT rand() as c1, rand() as c2, rand() as c3)";
        String filterQueryWhere = "WHERE c1 = rand() OR " + Joiner.on(" AND ").join(nCopies(250, "c1 = rand()"))
                + " OR " + Joiner.on(" AND ").join(nCopies(200, " c2 = rand()"))
                + " OR " + Joiner.on(" AND ").join(nCopies(200, " c3 = rand()"));

        assertPrestoExceptionThrownBy(() -> runner.execute("SELECT * " + filterQueryInner + filterQueryWhere))
                .hasErrorCode(COMPILER_ERROR)
                .hasMessageStartingWith("Query exceeded maximum filters");
    }
}
