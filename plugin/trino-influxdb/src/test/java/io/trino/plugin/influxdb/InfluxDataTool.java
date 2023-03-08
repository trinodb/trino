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

import com.google.common.collect.ImmutableList;
import org.influxdb.dto.Point;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class InfluxDataTool
{
    public static final String TEST_DATABASE = "test_database";
    public static final String TEST_DATABASE_ANOTHER = "test_database_another";
    public static final String TEST_MEASUREMENT_X = "test_measurement_x";
    public static final String TEST_MEASUREMENT_Y = "test_measurement_y";
    public static final String TEST_MEASUREMENT_Z = "test_measurement_z";

    private static final String CREATE_DATABASE_CMD = "CREATE DATABASE %s";

    private final InfluxSession session;

    public InfluxDataTool(InfluxSession session)
    {
        this.session = session;
    }

    public void setUpDatabase()
    {
        this.setUpDatabase(ImmutableList.of(TEST_DATABASE, TEST_DATABASE_ANOTHER));
    }

    public void setUpDatabase(List<String> databaseNames)
    {
        for (String databaseName : databaseNames) {
            String command = String.format(CREATE_DATABASE_CMD, databaseName);
            session.execute(command);
        }
    }

    public void setUpDataForTest()
    {
        this.setUpDataForTest(TEST_DATABASE, ImmutableList.of(TEST_MEASUREMENT_X, TEST_MEASUREMENT_Y));
        this.setUpDataForTest(TEST_DATABASE_ANOTHER, ImmutableList.of(TEST_MEASUREMENT_Z));
    }

    public void setUpDataForTest(String databaseName, List<String> tableNames)
    {
        String[] countries = Locale.getISOCountries();
        long start = System.currentTimeMillis() - 100_1000L;
        for (String tableName : tableNames) {
            for (int i = 0; i < 100; i++) {
                Point.Builder builder = Point.measurement(tableName)
                        .tag("country", countries[new Random().nextInt(countries.length)])
                        .addField("f1", new Random().nextInt(100))
                        .addField("f2", Math.round(new Random().nextDouble() * 100 * 100) / 100.0)
                        .addField("f3", "data_" + new Random(100))
                        .addField("f4", new Random().nextBoolean())
                        .time(start + i * 1000, TimeUnit.MILLISECONDS);
                session.write(databaseName, builder.build());
            }
        }
    }
}
