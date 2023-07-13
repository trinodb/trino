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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.server.PrefixObjectNameGeneratorModule;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static io.trino.execution.QueryRunnerUtil.cancelQuery;
import static io.trino.execution.QueryRunnerUtil.createQuery;
import static io.trino.execution.QueryRunnerUtil.waitForQueryState;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestExecutionJmxMetrics
{
    private static final String LONG_RUNNING_QUERY = "SELECT COUNT(*) FROM tpch.sf100000.lineitem";

    @Test(timeOut = 30_000)
    public void testQueryStats()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setAdditionalModule(new PrefixObjectNameGeneratorModule("io.trino"))
                .build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            InternalResourceGroupManager<?> resourceGroupManager = queryRunner.getCoordinator().getResourceGroupManager()
                    .orElseThrow(() -> new IllegalStateException("Resource manager not configured"));
            resourceGroupManager.setConfigurationManager(
                    "file",
                    ImmutableMap.of(
                            "resource-groups.config-file",
                            getClass().getClassLoader().getResource("resource_groups_single_query.json").getPath()));
            MBeanServer mbeanServer = queryRunner.getCoordinator().getMbeanServer();

            QueryId firstDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_RUNNING_QUERY);
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            assertEquals(getMbeanAttribute(mbeanServer, "RunningQueries"), 1);
            assertEquals(getMbeanAttribute(mbeanServer, "QueuedQueries"), 0);

            // the second "dashboard" query can't run right away because the resource group has a hardConcurrencyLimit of 1
            QueryId secondDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_RUNNING_QUERY);
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            assertEquals(getMbeanAttribute(mbeanServer, "RunningQueries"), 1);
            assertEquals(getMbeanAttribute(mbeanServer, "QueuedQueries"), 1);

            cancelQuery(queryRunner, secondDashboardQuery);
            waitForQueryState(queryRunner, secondDashboardQuery, FAILED);

            assertEquals(getMbeanAttribute(mbeanServer, "RunningQueries"), 1);
            assertEquals(getMbeanAttribute(mbeanServer, "QueuedQueries"), 0);

            // cancel the running query to avoid polluting the logs with meaningless stack traces
            try {
                cancelQuery(queryRunner, firstDashboardQuery);
                waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
            }
            catch (Exception ignore) {
            }
        }
    }

    private Session dashboardSession()
    {
        return testSessionBuilder()
                .setSource("dashboard")
                .build();
    }

    private long getMbeanAttribute(MBeanServer mbeanServer, String attribute)
            throws Exception
    {
        return (Long) mbeanServer.getAttribute(new ObjectName("trino.execution:name=QueryManager"), attribute);
    }
}
