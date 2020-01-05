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
package io.prestosql.plugin.influx;

import org.testcontainers.containers.InfluxDBContainer;

import java.io.Closeable;

import static org.testcontainers.containers.InfluxDBContainer.INFLUXDB_PORT;

public class TestingInfluxServer
        implements Closeable
{
    public static final String DATABASE = "Test";
    public static final String RETENTION_POLICY = "Schema";
    public static final String MEASUREMENT = "Data";

    private final InfluxDBContainer dockerContainer;
    private final InfluxHttp influxClient;

    public TestingInfluxServer()
    {
        dockerContainer = new InfluxDBContainer()
                .withDatabase(DATABASE)
                .withAuthEnabled(false);
        dockerContainer.start();

        influxClient = new InfluxHttp(getHost(), getPort(), false, DATABASE, null, null);
        influxClient.execute("CREATE RETENTION POLICY " + RETENTION_POLICY + " ON " + DATABASE + " DURATION INF REPLICATION 1");
    }

    public InfluxHttp getInfluxClient()
    {
        return influxClient;
    }

    public String getHost()
    {
        return dockerContainer.getContainerIpAddress();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(INFLUXDB_PORT);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
