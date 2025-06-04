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
package io.trino.execution.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.Session;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.plugin.resourcegroups.ResourceGroupSelector;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfigurationManager;
import io.trino.plugin.resourcegroups.db.H2DaoProvider;
import io.trino.plugin.resourcegroups.db.H2ResourceGroupsDao;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.TERMINAL_QUERY_STATES;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.resourcegroups.QueryType.EXPLAIN;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class H2TestUtil
{
    private static final String CONFIGURATION_MANAGER_TYPE = "h2";
    public static final String TEST_ENVIRONMENT = "test_environment";
    public static final String TEST_ENVIRONMENT_2 = "test_environment_2";
    public static final JsonCodec<List<String>> CLIENT_TAGS_CODEC = listJsonCodec(String.class);

    private H2TestUtil() {}

    public static Session adhocSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("adhoc")
                .build();
    }

    public static Session dashboardSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("dashboard")
                .build();
    }

    public static Session rejectingSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("reject")
                .build();
    }

    public static void waitForCompleteQueryCount(QueryRunner queryRunner, int expectedCount)
            throws InterruptedException
    {
        waitForQueryCount(queryRunner, TERMINAL_QUERY_STATES, expectedCount);
    }

    public static void waitForRunningQueryCount(QueryRunner queryRunner, int expectedCount)
            throws InterruptedException
    {
        waitForQueryCount(queryRunner, ImmutableSet.of(RUNNING), expectedCount);
    }

    public static void waitForQueryCount(QueryRunner queryRunner, Set<QueryState> countingStates, int expectedCount)
            throws InterruptedException
    {
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        while (queryManager.getQueries().stream()
                .filter(q -> countingStates.contains(q.getState())).count() != expectedCount) {
            MILLISECONDS.sleep(500);
        }
    }

    public static String getDbConfigUrl()
    {
        return "jdbc:h2:mem:test_" + Math.abs(new Random().nextLong()) + ";NON_KEYWORDS=KEY,VALUE"; // key and value are reserved keywords in H2 2.x
    }

    public static H2ResourceGroupsDao getDao(String url)
    {
        DbResourceGroupConfig dbResourceGroupConfig = new DbResourceGroupConfig()
                .setConfigDbUrl(url);
        H2ResourceGroupsDao dao = new H2DaoProvider(dbResourceGroupConfig).get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.createResourceGroupsGlobalPropertiesTable();
        return dao;
    }

    public static QueryRunner createQueryRunner(String dbConfigUrl, H2ResourceGroupsDao dao)
            throws Exception
    {
        return createQueryRunner(dbConfigUrl, dao, TEST_ENVIRONMENT);
    }

    public static QueryRunner createQueryRunner(String dbConfigUrl, H2ResourceGroupsDao dao, String environment)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner
                .builder(testSessionBuilder().setCatalog("tpch").setSchema("tiny").build())
                .setWorkerCount(1)
                .setEnvironment(environment)
                .build();
        try {
            Plugin h2ResourceGroupManagerPlugin = new H2ResourceGroupManagerPlugin();
            queryRunner.installPlugin(h2ResourceGroupManagerPlugin);
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager(CONFIGURATION_MANAGER_TYPE, ImmutableMap.of("resource-groups.config-db-url", dbConfigUrl, "node.environment", environment));
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            setup(queryRunner, dao, environment);
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static QueryRunner getSimpleQueryRunner()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        return createQueryRunner(dbConfigUrl, dao);
    }

    private static void setup(QueryRunner queryRunner, H2ResourceGroupsDao dao, String environment)
            throws InterruptedException
    {
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertResourceGroup(1, "global", "1MB", 100, 1000, 1000, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertResourceGroup(2, "bi-${USER}", "1MB", 3, 2, 2, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(3, "user-${USER}", "1MB", 3, 3, 3, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(4, "adhoc-${USER}", "1MB", 3, 3, 3, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(5, "dashboard-${USER}", "1MB", 1, 1, 1, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(6, "no-queueing", "1MB", 0, 1, 1, null, null, null, null, null, null, TEST_ENVIRONMENT_2);
        dao.insertResourceGroup(7, "explain", "1MB", 0, 1, 1, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertSelector(2, 10_000, "user.*", null, null, null, "test", null, null, null);
        dao.insertSelector(4, 1_000, "user.*", null, null, null, "(?i).*adhoc.*", null, null, null);
        dao.insertSelector(5, 100, "user.*", null, null, null, "(?i).*dashboard.*", null, null, null);
        dao.insertSelector(4, 10, "user.*", null, null, null, null, null, CLIENT_TAGS_CODEC.toJson(ImmutableList.of("tag1", "tag2")), null);
        dao.insertSelector(2, 1, "user.*", null, null, null, null, null, CLIENT_TAGS_CODEC.toJson(ImmutableList.of("tag1")), null);
        dao.insertSelector(6, 6, ".*", null, null, null, ".*", null, null, null);
        dao.insertSelector(7, 100_000, null, null, null, null, null, EXPLAIN.name(), null, null);

        int expectedSelectors = 6;
        if (environment.equals(TEST_ENVIRONMENT_2)) {
            expectedSelectors = 1;
        }

        // Selectors are loaded last
        while (getSelectors(queryRunner).size() != expectedSelectors) {
            MILLISECONDS.sleep(500);
        }
    }

    public static List<ResourceGroupSelector> getSelectors(QueryRunner queryRunner)
    {
        try {
            return ((DbResourceGroupConfigurationManager) queryRunner.getCoordinator().getResourceGroupManager().get().getConfigurationManager()).getSelectors();
        }
        catch (TrinoException e) {
            if (e.getErrorCode() == CONFIGURATION_INVALID.toErrorCode()) {
                return ImmutableList.of();
            }

            throw e;
        }
    }
}
