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
package io.trino.memory;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMemorySessionProperties
        extends AbstractTestQueryFramework
{
    public static final String sql = "SELECT COUNT(*), clerk FROM orders GROUP BY clerk";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().setNodeCount(2).build();
    }

    @Test(timeOut = 240_000)
    public void testSessionQueryMemoryPerNodeLimit()
    {
        assertQuery(sql);
        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1kB")
                .build();
        assertThatThrownBy(() -> getQueryRunner().execute(session, sql))
                .isInstanceOf(RuntimeException.class)
                .hasMessageStartingWith("Query exceeded per-node memory limit of ");
    }
}
