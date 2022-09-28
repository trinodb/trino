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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Paths;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributedSpilledQueries
        extends AbstractTestQueries
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createSpillingQueryRunner();
    }

    public static DistributedQueryRunner createSpillingQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "2")
                .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
                .setSystemProperty(SystemSessionProperties.AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, "128kB")
                .build();

        ImmutableMap<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "trino", "spills").toString())
                .put("spiller-max-used-space-threshold", "1.0")
                .put("memory-revoking-threshold", "0.0") // revoke always
                .put("memory-revoking-target", "0.0")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    // The spilling does not happen deterministically. TODO improve query and configuration so that it does.
    @Test(invocationCount = 10, successPercentage = 20)
    public void testExplainAnalyzeReportSpilledDataSize()
    {
        assertThat((String) computeActual("EXPLAIN ANALYZE SELECT sum(custkey) OVER (PARTITION BY orderkey) FROM orders").getOnlyValue())
                .containsPattern(", Spilled: [1-9][0-9]*\\wB");
    }
}
