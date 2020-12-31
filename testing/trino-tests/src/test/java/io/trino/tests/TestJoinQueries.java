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
package io.prestosql.tests;

import io.prestosql.execution.DynamicFilterConfig;
import io.prestosql.testing.AbstractTestJoinQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static com.google.common.base.Verify.verify;

/**
 * @see TestJoinQueriesWithoutDynamicFiltering for tests with dynamic filtering disabled
 */
public class TestJoinQueries
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void verifyDynamicFilteringEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'enable_dynamic_filtering'",
                "VALUES ('enable_dynamic_filtering', 'true', 'true', 'boolean', 'Enable dynamic filtering')");
    }
}
