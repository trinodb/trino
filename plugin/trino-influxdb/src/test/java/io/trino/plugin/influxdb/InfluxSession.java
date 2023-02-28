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

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.io.Closeable;

import static io.trino.plugin.influxdb.TestingInfluxServer.PASSWORD;
import static io.trino.plugin.influxdb.TestingInfluxServer.USERNAME;

public class InfluxSession
        implements Closeable
{
    private final InfluxDB influxDB;

    public InfluxSession(String endpoint)
    {
        this.influxDB = InfluxDBFactory.connect(endpoint, USERNAME, PASSWORD);
        this.influxDB.disableBatch();
    }

    public String checkConnectivity()
    {
        return influxDB.version();
    }

    public void execute(String command)
    {
        execute(null, command);
    }

    public void execute(String database, String command)
    {
        Query query = database == null ?
                new Query(command) : new Query(command, database);
        influxDB.query(query);
    }

    public void write(String database, Point point)
    {
        influxDB.setDatabase(database);
        influxDB.write(point);
    }

    @Override
    public void close()
    {
        influxDB.close();
    }
}
