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

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static java.lang.String.format;

public class TestingIgniteContainer
        extends JdbcDatabaseContainer<TestingIgniteContainer>
{
    private static final String IGNITE_SQL_MERGE_TABLE_MAX_SIZE = "20000";

    public TestingIgniteContainer()
    {
        super(DockerImageName.parse("apacheignite/ignite:2.8.0"));
        this.withExposedPorts(10800);
        this.withEnv("IGNITE_SQL_MERGE_TABLE_MAX_SIZE", IGNITE_SQL_MERGE_TABLE_MAX_SIZE).withStartupAttempts(10);
        this.waitingFor((new HttpWaitStrategy()).forStatusCode(200).forResponsePredicate("Ok."::equals).withStartupTimeout(Duration.ofMinutes(1L)));
    }

    @Override
    public String getDriverClassName()
    {
        return "org.apache.ignite.IgniteJdbcThinDriver";
    }

    @Override
    public String getJdbcUrl()
    {
        return format("jdbc:ignite:thin://%s:%s/public", getHost(), getMappedPort(10800));
    }

    @Override
    public String getUsername()
    {
        return "default";
    }

    @Override
    public String getPassword()
    {
        return "";
    }

    @Override
    public String getTestQueryString()
    {
        return "SELECT 1";
    }

    @Override
    public TestingIgniteContainer withUrlParam(String paramName, String paramValue)
    {
        throw new UnsupportedOperationException("The ignite does not support this");
    }
}
