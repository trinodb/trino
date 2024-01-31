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
package io.trino.plugin.starrocks;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static java.lang.String.format;

public class TestingStarRocksContainer
        extends JdbcDatabaseContainer<TestingStarRocksContainer>
{
    private static final String STARROCKS_IMAGE = "starrocks/allin1-ubuntu:2.5.6";

    public TestingStarRocksContainer()
    {
        super(DockerImageName.parse(STARROCKS_IMAGE));
        this.withExposedPorts(9030)
                .waitingFor(
                        new HttpWaitStrategy()
                                .forStatusCode(200)
                                .forResponsePredicate("Ok."::equals)
                                .withStartupTimeout(Duration.ofMinutes(1L)));
    }

    @Override
    public String getDriverClassName()
    {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl()
    {
        return format("jdbc:mysql://%s:%s", getHost(), getMappedPort(9030));
    }

    @Override
    public String getUsername()
    {
        return "root";
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
    public TestingStarRocksContainer withUrlParam(String paramName, String paramValue)
    {
        throw new UnsupportedOperationException("The StarRocks does not support this");
    }
}
