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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestJoinQueries;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.tpch.TpchTable.getTables;

/**
 * @see TestHiveDistributedJoinQueries for tests with dynamic filtering enabled
 */
public class TestHiveDistributedJoinQueriesWithoutDynamicFiltering
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(getTables())
                .setExtraProperties(ImmutableMap.of("enable-dynamic-filtering", "false"))
                .build();
    }

    @Test
    public void verifyDynamicFilteringDisabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'enable_dynamic_filtering'",
                "VALUES ('enable_dynamic_filtering', 'false', 'false', 'boolean', 'Enable dynamic filtering')");
    }
}
