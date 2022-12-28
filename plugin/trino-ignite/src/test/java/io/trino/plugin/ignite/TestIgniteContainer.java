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

public class TestIgniteContainer
        extends JdbcDatabaseContainer
{
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("apacheignite/ignite");

    public static final Integer HTTP_PORT = 10800;

    private static final String NAME = "ignite";
    private static final Integer NATIVE_PORT = 18000;
    private static final String DRIVER_CLASS_NAME = "org.apache.ignite.IgniteJdbcThinDriver";
    private static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";
    private static final String TEST_QUERY = "SELECT 1";
    private final String databaseName;
    private final String username;
    private final String password;

    public TestIgniteContainer(String dockerImageName)
    {
        this(DockerImageName.parse(dockerImageName));
    }

    public TestIgniteContainer(DockerImageName dockerImageName)
    {
        super(dockerImageName);
        this.databaseName = "public";
        this.username = "default";
        this.password = "";
        dockerImageName.assertCompatibleWith(new DockerImageName[] {DEFAULT_IMAGE_NAME});
        this.withExposedPorts(new Integer[] {HTTP_PORT, NATIVE_PORT});
        this.waitingFor((new HttpWaitStrategy()).forStatusCode(200).forResponsePredicate("Ok."::equals).withStartupTimeout(Duration.ofMinutes(1L)));
    }

    @Override
    public String getDriverClassName()
    {
        return DRIVER_CLASS_NAME;
    }

    @Override
    public String getJdbcUrl()
    {
        return JDBC_URL_PREFIX + this.getHost() + ":" + this.getMappedPort(HTTP_PORT) + "/" + this.databaseName;
    }

    @Override
    public String getUsername()
    {
        return this.username;
    }

    @Override
    public String getPassword()
    {
        return this.password;
    }

    @Override
    public String getTestQueryString()
    {
        return TEST_QUERY;
    }

    @Override
    public TestIgniteContainer withUrlParam(String paramName, String paramValue)
    {
        throw new UnsupportedOperationException("The " + NAME + " does not support this");
    }
}
