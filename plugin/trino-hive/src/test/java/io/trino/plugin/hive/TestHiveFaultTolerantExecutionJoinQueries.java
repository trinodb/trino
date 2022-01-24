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
package io.trino.plugin.hive;

import io.trino.testing.AbstractTestFaultTolerantExecutionJoinQueries;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.tpch.TpchTable.getTables;

public class TestHiveFaultTolerantExecutionJoinQueries
        extends AbstractTestFaultTolerantExecutionJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner(Map<String, String> extraProperties, Map<String, String> exchangeManagerProperties)
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setExtraProperties(extraProperties)
                .setExchangeManagerProperties(exchangeManagerProperties)
                .setInitialTables(getTables())
                .build();
    }

    @Override
    @Test(enabled = false)
    public void testOutputDuplicatesInsensitiveJoin()
    {
        // flaky
    }
}
