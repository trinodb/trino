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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TestingInfluxServer
{
    public static final String DATABASE = "Test";
    public static final String RETENTION_POLICY = "Schema";
    public static final String MEASUREMENT = "Data";
    private static final Instant[] T = new Instant[] {
            Instant.parse("2019-12-10T21:00:04.446Z"),
            Instant.parse("2019-12-10T21:00:20.446Z"),
            Instant.parse("2019-12-10T22:00:04.446Z"),
    };
    private static final String[] K1 = new String[] {"a", "b", "d"};
    private static final String[] K2 = new String[] {"b", "c", "b"};
    private static final double[] V1 = new double[] {1, 3, 5};
    private static final double[] V2 = new double[] {2, 4, 6};

    private final InfluxDBContainer dockerContainer;

    public TestingInfluxServer()
    {
        dockerContainer = new InfluxDBContainer()
                .withDatabase(DATABASE)
                .withAuthEnabled(false);
        dockerContainer.start();

        InfluxHttp http = new InfluxHttp(getHost(), getPort(), false, DATABASE, null, null);
        //http.executeDDL("CREATE DATABASE " + DATABASE);
        http.execute("CREATE RETENTION POLICY " + RETENTION_POLICY + " ON " + DATABASE + " DURATION INF REPLICATION 1");
        http.write(RETENTION_POLICY, getWrite(0), getWrite(1), getWrite(2));
    }

    public String getHost()
    {
        return dockerContainer.getContainerIpAddress();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(InfluxDBContainer.INFLUXDB_PORT);
    }

    public static String getWrite(int row)
    {
        return MEASUREMENT + ",k1=" + K1[row] + ",k2=" + K2[row] + " v1=" + V1[row] + ",v2=" + V2[row] + " " + T[row].toEpochMilli() + "000000";
    }

    public static Object[] getColumns(int row)
    {
        return new Object[] {ZonedDateTime.ofInstant(T[row], ZoneId.of("UTC")), K1[row], K2[row], V1[row], V2[row]};
    }

    public void close()
    {
        dockerContainer.close();
    }
}
