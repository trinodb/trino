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
package com.qubole.presto.kinesis;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.qubole.presto.kinesis.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test session variable utilities and ensure connector is defining the session variables.
 */
public class TestSessionVariables
{
    private Session protoSession;
    private ConnectorSession session;
    private SessionPropertyManager propManager = new SessionPropertyManager();
    private Injector injector;

    /*protected void setProperty(String name, String value)
    {
        protoSession = protoSession.withCatalogProperty("kinesis", name, value);
        session = protoSession.toConnectorSession(new ConnectorId("kinesis"));
    }*/

    protected ConnectorSession makeSessionWithTimeZone(String tzId)
    {
        return Session.builder(propManager)
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource("source")
                .setCatalog("kinesis")
                .setSchema("default")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(tzId))
                .setLocale(ENGLISH)
                .setQueryId(new QueryId("dummy"))
                .build().toConnectorSession(new ConnectorId("kinesis"));
    }

    @BeforeClass
    public void start()
    {
        // Create dependent objects, including the minimal config needed for this test
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-dir", "etc/kinesis")
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .build();

        KinesisPlugin kinesisPlugin = TestUtils.createPluginInstance();
        KinesisConnector connector = TestUtils.createConnector(kinesisPlugin, properties, true);
        injector = kinesisPlugin.getInjector();
        assertNotNull(injector);

        protoSession = Session.builder(propManager)
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource("source")
                .setCatalog("kinesis")
                .setSchema("default")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Los_Angeles"))
                .setLocale(ENGLISH)
                .setQueryId(new QueryId("dummy"))
                .build();
        session = protoSession.toConnectorSession(new ConnectorId("kinesis"));

        // Connector needs to tell Presto about the session properties it supports
        propManager.addConnectorSessionProperties(new ConnectorId("kinesis"), connector.getSessionProperties());
    }

    @Test
    public void testVariables()
    {
        // Verify that defaults are there when we don't set anything and that props are defined
        assertTrue(!SessionVariables.getCheckpointEnabled(session));
        assertTrue(SessionVariables.getIterFromTimestamp(session));
        assertEquals(SessionVariables.getBatchSize(session), 10000);
        assertEquals(SessionVariables.getMaxBatches(session), 600);
        assertEquals(SessionVariables.getIterOffsetSeconds(session), 86400);
        assertEquals(SessionVariables.getIterStartTimestamp(session), 0);

        /*// Set some things:
        setProperty("batch_size", "5000");
        setProperty("iter_offset_seconds", "43200");

        assertEquals(SessionVariables.getBatchSize(session), 5000);
        assertEquals(SessionVariables.getIterOffsetSeconds(session), 43200);*/
    }

    @Test
    public void testTimestampAccess()
    {
        // Test timestamp functions:
        TimeZoneKey tzKey = session.getTimeZoneKey();
        assertEquals(tzKey.getId(), "America/Los_Angeles");

        // Test a known result
        long result = SessionVariables.getTimestampAsLong("2016-07-10 12:03:56.124", session);
        assertTrue(result != 0);
        assertEquals(result, 1468177436124L);

        ConnectorSession altSession = makeSessionWithTimeZone("America/New_York");
        long result2 = SessionVariables.getTimestampAsLong("2016-07-10 12:03:56.124", altSession);
        assertTrue(result2 != 0);
        assertEquals(result2, 1468166636124L);
    }
}
