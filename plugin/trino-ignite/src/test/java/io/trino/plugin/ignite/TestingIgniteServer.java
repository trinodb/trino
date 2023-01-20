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
package io.trino.plugin.ignite;

import io.trino.testing.ResourcePresence;
import io.trino.testing.SharedResource;

import javax.annotation.concurrent.GuardedBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TestingIgniteServer
        implements AutoCloseable
{
    // When doing the test, we may get multiple clusters on the same machine, they might (due to the overlap in some ports)
    // be considered as one cluster, "Data Rebalancing" between Ignite instances would happen immediately, the use of shared resources
    // here is to prevent "Data Rebalancing" in Ignite from causing test errors.
    @GuardedBy("this")
    private static final SharedResource<TestingIgniteServer> sharedResource = new SharedResource<>(TestingIgniteServer::new);

    private final TestingIgniteContainer dockerContainer;

    public static synchronized SharedResource.Lease<TestingIgniteServer> getInstance()
            throws Exception
    {
        return sharedResource.getInstanceLease();
    }

    private TestingIgniteServer()
    {
        dockerContainer = new TestingIgniteContainer();

        dockerContainer.start();
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl()
    {
        return dockerContainer.getJdbcUrl();
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return dockerContainer.getContainerId() != null;
    }
}
