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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cache.CacheConfig;
import io.trino.testing.PlanTester;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.TestHiveTpchCostBasedPlan.TPCH_METADATA_DIR;
import static io.trino.testing.PlanTesterBuilder.planTesterBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPC-H queries.
 * This class is using Hive connector with mocked in memory thrift metastore with unpartitioned TPC-H tables.
 */
public class TestCachingHiveTpchCostBasedPlan
        extends BaseHiveCostBasedPlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestCachingHiveTpchCostBasedPlan()
    {
        super(TPCH_METADATA_DIR, false);
    }

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(schemaName)
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .setSystemProperty(JOIN_REORDERING_STRATEGY, OptimizerConfig.JoinReorderingStrategy.AUTOMATIC.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, OptimizerConfig.JoinDistributionType.AUTOMATIC.name());
        PlanTester planTester = planTesterBuilder(sessionBuilder.build())
                .withNodeCountForStats(8)
                .withCacheConfig(new CacheConfig()
                        .setEnabled(true)
                        .setCacheCommonSubqueriesEnabled(true))
                .build();
        planTester.createCatalog(
                CATALOG_NAME,
                createConnectorFactory(),
                ImmutableMap.of());
        return planTester;
    }

    @Override
    protected String getQueryPlanResourcePath(String queryResourcePath)
    {
        Path queryPath = Paths.get(queryResourcePath);
        Path directory = queryPath.getParent();
        directory = directory.resolve("hive").resolve("cache");
        String planResourceName = queryPath.getFileName().toString().replaceAll("\\.sql$", ".plan.txt");
        return directory.resolve(planResourceName).toString();
    }

    @Override
    protected List<String> getQueryResourcePaths()
    {
        return TPCH_SQL_FILES;
    }

    public static void main(String[] args)
    {
        new TestCachingHiveTpchCostBasedPlan().generate();
    }
}
