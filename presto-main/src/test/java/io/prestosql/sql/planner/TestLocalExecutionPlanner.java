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
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
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
        runner = new LocalQueryRunner(TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        closeAllRuntimeException(runner);
        runner = null;
    }

    @Test
    public void testCompilerFailure()
    {
        // structure the query this way to avoid stack overflow when parsing
        String inner = "(" + Joiner.on(" + ").join(nCopies(100, "rand()")) + ")";
        String outer = Joiner.on(" + ").join(nCopies(100, inner));
        assertFails("SELECT " + outer, COMPILER_ERROR);
    }

    private void assertFails(@Language("SQL") String sql, ErrorCodeSupplier supplier)
    {
        assertPrestoExceptionThrownBy(() -> runner.execute(sql))
                .hasErrorCode(supplier);
    }
}
