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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.testing.AbstractDynamicPartitionPruningIntegrationSmokeTest;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.tpch.TpchTable.getTables;
import static java.lang.String.format;

public class TestHiveDynamicPartitionPruning
        extends AbstractDynamicPartitionPruningIntegrationSmokeTest
{
    private static final Logger log = Logger.get(TestHiveDynamicPartitionPruning.class);
    private static final String PARTITIONED_LINEITEM = "partitioned_lineitem";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                // Reduced partitioned join limit for large DF to enable DF min/max collection with ENABLE_LARGE_DYNAMIC_FILTERS
                .addExtraProperty("dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "100")
                .addExtraProperty("dynamic-filtering.large-partitioned.range-row-limit-per-driver", "100000")
                .setHiveProperties(ImmutableMap.of("hive.dynamic-filtering-probe-blocking-timeout", "1h"))
                .setInitialTables(getTables())
                .build();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        // setup partitioned fact table for dynamic partition pruning
        @Language("SQL") String sql = format("CREATE TABLE %s WITH (format = 'TEXTFILE', partitioned_by=array['suppkey']) AS " +
                "SELECT orderkey, partkey, suppkey FROM %s", PARTITIONED_LINEITEM, "tpch.tiny.lineitem");
        long start = System.nanoTime();
        long rows = (Long) getQueryRunner().execute(sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, PARTITIONED_LINEITEM, nanosSince(start).convertToMostSuccinctTimeUnit());
    }
}
