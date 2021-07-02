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

import io.trino.testing.AbstractDynamicFilteringIntegrationSmokeTest;
import io.trino.testing.QueryRunner;

import static io.trino.tpch.TpchTable.getTables;

public class TestHiveDynamicFiltering
        extends AbstractDynamicFilteringIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                // Adjust DF limits to test edge cases
                .addExtraProperty("dynamic-filtering.small-broadcast.max-distinct-values-per-driver", "100")
                .addExtraProperty("dynamic-filtering.small-broadcast.range-row-limit-per-driver", "100")
                .addExtraProperty("dynamic-filtering.large-broadcast.max-distinct-values-per-driver", "100")
                .addExtraProperty("dynamic-filtering.large-broadcast.range-row-limit-per-driver", "100000")
            .setInitialTables(getTables())
            .build();
    }
}
