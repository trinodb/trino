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

package io.trino.plugin.influxdb;

import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.InfluxDBContainer;
import org.testcontainers.utility.DockerImageName;

import static com.google.common.base.Strings.isNullOrEmpty;

public class TestingInfluxServer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(TestingInfluxServer.class);

    public static final String VERSION = "1.8.10";
    public static final String USERNAME = "admin";
    public static final String PASSWORD = "password";
    private static final int PORT = 8086;
    private InfluxDBContainer<?> dockerContainer;

    public TestingInfluxServer()
    {
        this(VERSION);
    }

    public TestingInfluxServer(String influxVersion)
    {
        log.info("Influx server starting...");

        DockerImageName dockerImageName = DockerImageName.parse("influxdb").withTag(influxVersion);
        try {
            this.dockerContainer = new InfluxDBContainer<>(dockerImageName)
                    .withExposedPorts(PORT)
                    .withAdmin(USERNAME)
                    .withAdminPassword(PASSWORD)
                    .withUsername("test")
                    .withPassword("password")
                    .withNetworkAliases("influxdb");
            this.dockerContainer.start();

            try (InfluxSession session = new InfluxSession(getEndpoint())) {
                String version = session.checkConnectivity();
                log.info("Influx server connectivity check is %s.", !isNullOrEmpty(version) ? "success" : "fail");
            }
        }
        catch (Exception e) {
            if (this.dockerContainer != null) {
                this.dockerContainer.close();
            }
            throw e;
        }

        log.info("Influx server started.");
    }

    public String getEndpoint()
    {
        return this.dockerContainer.getUrl();
    }

    @Override
    public void close()
    {
        this.dockerContainer.close();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return dockerContainer.getContainerId() != null;
    }
}
