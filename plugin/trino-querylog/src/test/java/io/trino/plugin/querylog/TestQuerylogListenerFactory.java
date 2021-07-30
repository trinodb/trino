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
package io.trino.plugin.querylog;

import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

@Test(singleThreaded = true)
public class TestQuerylogListenerFactory
{
    private final QuerylogListenerFactory factory = new QuerylogListenerFactory();

    @Test
    public void testGoodConfig_ShouldReturn_EventListener()
    {
        factory.create(
                new HashMap<>() {{
                    put("trino.querylog.connect.port", "8090");
                    put("trino.querylog.connect.inet.address", "google.com");
                    put("trino.querylog.connect.scheme", "http");
                }});
    }

    @Test
    public void testNameNotEmptyOrNull()
    {
        assertTrue(factory.getName() != null && !factory.getName().trim().isEmpty(), "plugin name cannot be null or empty");
    }

    @Test
    public void testBadIngestURLConfig_ShouldThrow_IllegalArgumentException()
    {
        inetConfigTestUtil(null);
        inetConfigTestUtil("");
        inetConfigTestUtil("this_is-not*an+url");
    }

    private void inetConfigTestUtil(String inetAddress)
    {
        Exception missingInetException = expectThrows(IllegalArgumentException.class, () -> factory.create(
                new HashMap<>() {{
                    put("trino.querylog.connect.port", "8090");
                    put("trino.querylog.connect.inet.address", inetAddress);
                    put("trino.querylog.connect.scheme", "http");
                }}));
        assertTrue(missingInetException.getMessage().contains("trino.querylog.connect.inet.address"), String.format("error message should reference the config field: %s", missingInetException.getMessage()));
    }

    @Test
    public void testBadPortConfig_ShouldThrow_IllegalArgumentException()
    {
        portConfigTestUtil("NAN");
        portConfigTestUtil("-100");
        portConfigTestUtil("");
        portConfigTestUtil(null);
    }

    private void portConfigTestUtil(String port)
    {
        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(
                new HashMap<>() {{
                    put("trino.querylog.connect.inet.address", "google.com");
                    put("trino.querylog.connect.port", port);
                    put("trino.querylog.connect.scheme", "http");
                }}));
        assertTrue(e.getMessage().contains("trino.querylog.connect.port"));
    }

    @Test
    public void testBadSchemeConfig_ShouldThrow_IllegalArgumentException()
    {
        schemeConfigTestUtil(null);
        schemeConfigTestUtil("");
        schemeConfigTestUtil("not a valid scheme");
    }

    private void schemeConfigTestUtil(String scheme)
    {
        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(
                new HashMap<>() {{
                    put("trino.querylog.connect.inet.address", "google.com");
                    put("trino.querylog.connect.port", "8090");
                    put("trino.querylog.connect.scheme", scheme);
                }}));
        assertTrue(e.getMessage().contains("trino.querylog.connect.scheme"));
    }

    @Test
    public void testHttpHeadersConfig_ShouldReturn_CorrectHeaders()
            throws Exception
    {
        PrivateObjectFieldAccessor<Map<String, String>> httpHeadersFieldAccessor = new PrivateObjectFieldAccessor<>("httpHeaders");

        QuerylogListener querylogListener = (QuerylogListener) factory.create(new HashMap<>() {{
                put("trino.querylog.connect.inet.address", "google.com");
                put("trino.querylog.connect.port", "8090");
                put("trino.querylog.connect.scheme", "http");
                put("trino.querylog.connect.headers.Authorization", "trust-me");
                put("trino.querylog.connect.headers.Cache-Control", "no-cache");
            }});
        assertEquals(httpHeadersFieldAccessor.getPrivateFieldFrom(querylogListener).get("Authorization"), "trust-me", "http header not set from configuration");
        assertEquals(httpHeadersFieldAccessor.getPrivateFieldFrom(querylogListener).get("Cache-Control"), "no-cache", "http header not set from configuration");

        querylogListener = (QuerylogListener) factory.create(new HashMap<>() {{
                put("trino.querylog.connect.inet.address", "google.com");
                put("trino.querylog.connect.port", "8090");
                put("trino.querylog.connect.scheme", "http");
            }});
        assertNotNull(httpHeadersFieldAccessor.getPrivateFieldFrom(querylogListener), "http headers must not be null, even when no configuration was given");
    }

    @Test
    public void testEnabledEventsConfig_ShouldReturn_CorrectEnables()
            throws Exception
    {
        PrivateBooleanFieldAccessor completedFieldAccessor = new PrivateBooleanFieldAccessor("shouldLogCompleted");
        PrivateBooleanFieldAccessor createdFieldAccessor = new PrivateBooleanFieldAccessor("shouldLogCreated");
        PrivateBooleanFieldAccessor splitFieldAccessor = new PrivateBooleanFieldAccessor("shouldLogSplit");

        QuerylogListener querylogListener = (QuerylogListener) factory.create(new HashMap<>() {{
                put("trino.querylog.connect.inet.address", "google.com");
                put("trino.querylog.connect.port", "8090");
                put("trino.querylog.connect.scheme", "http");
                put("trino.querylog.log.created", "");
                put("trino.querylog.log.completed", "true");
                put("trino.querylog.log.split", "True");
            }});
        assertTrue(createdFieldAccessor.getPrivateFieldFrom(querylogListener), "NOT logging created events when configuration is \"\"");
        assertTrue(completedFieldAccessor.getPrivateFieldFrom(querylogListener), "NOT logging completed events when configuration is \"true\"");
        assertTrue(splitFieldAccessor.getPrivateFieldFrom(querylogListener), "NOT logging split events when configuration is \"True\"");

        querylogListener = (QuerylogListener) factory.create(new HashMap<>() {{
                put("trino.querylog.connect.inet.address", "google.com");
                put("trino.querylog.connect.port", "8090");
                put("trino.querylog.connect.scheme", "http");
                put("trino.querylog.log.created", "false");
                put("trino.querylog.log.completed", "FALSE");
            }});
        assertFalse(createdFieldAccessor.getPrivateFieldFrom(querylogListener), "logging created events when configuration is \"false\"");
        assertFalse(completedFieldAccessor.getPrivateFieldFrom(querylogListener), "logging completed events when configuration is \"FALSE\"");
        assertFalse(splitFieldAccessor.getPrivateFieldFrom(querylogListener), "logging split events when configuration is not present");
    }

    private class PrivateObjectFieldAccessor<T>
    {
        private final String fieldName;

        public PrivateObjectFieldAccessor(String fieldName)
        {
            this.fieldName = fieldName;
        }

        public T getPrivateFieldFrom(Object obj)
                throws Exception
        {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(obj);
        }
    }

    private class PrivateBooleanFieldAccessor
    {
        private final String fieldName;

        public PrivateBooleanFieldAccessor(String fieldName)
        {
            this.fieldName = fieldName;
        }

        public boolean getPrivateFieldFrom(Object obj)
                throws Exception
        {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.getBoolean(obj);
        }
    }
}
