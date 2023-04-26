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
import io.trino.spi.connector.RecordSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.plugin.influxdb.InfluxDataTool.TEST_DATABASE;
import static io.trino.plugin.influxdb.InfluxDataTool.TEST_MEASUREMENT_X;
import static io.trino.plugin.influxdb.InfluxTransactionHandle.INSTANCE;
import static io.trino.plugin.influxdb.TestingInfluxServer.PASSWORD;
import static io.trino.plugin.influxdb.TestingInfluxServer.USERNAME;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestInfluxRecordSetProvider
{
    private static TestingInfluxServer server;
    private static InfluxClient client;
    private static InfluxSplit split;

    @BeforeClass
    public void setupServer()
    {
        server = new TestingInfluxServer();
        InfluxConfig config = new InfluxConfig();
        config.setEndpoint(server.getEndpoint());
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        client = new NativeInfluxClient(config);
        split = new InfluxSplit();

        try (InfluxSession session = new InfluxSession(server.getEndpoint())) {
            InfluxDataTool tool = new InfluxDataTool(session);
            tool.setUpDatabase();
            tool.setUpDataForTest();
        }
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
        server = null;
    }

    @Test
    public void testGetRecordSet()
    {
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE, TEST_MEASUREMENT_X);
        InfluxRecordSetProvider provider = new InfluxRecordSetProvider(client);
        RecordSet recordSet = provider.getRecordSet(INSTANCE, SESSION, split, tableHandle, ImmutableList.of());
        InfluxRecordSet influxRecordSet = (InfluxRecordSet) recordSet;
        assertNotNull(influxRecordSet);
        assertNotNull(influxRecordSet.getSourceData());
        assertTrue(influxRecordSet.getSourceData().getColumns().containsAll(
                ImmutableList.of("time", "f1", "f2", "f3", "f4", "country")));
        assertEquals(influxRecordSet.getSourceData().getValues().size(), 100);
    }
}
