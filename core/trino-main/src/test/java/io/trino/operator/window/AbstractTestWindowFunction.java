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
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SessionTestUtils.TEST_SESSION;
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
