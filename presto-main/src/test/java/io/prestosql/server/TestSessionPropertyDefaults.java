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
package io.prestosql.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.session.SessionPropertyConfigurationManagerFactory;
import io.prestosql.spi.session.TestingSessionPropertyConfigurationManagerFactory;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.SystemSessionProperties.EXECUTION_POLICY;
import static io.prestosql.SystemSessionProperties.HASH_PARTITION_COUNT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSessionPropertyDefaults
{
    private static final ResourceGroupId TEST_RESOURCE_GROUP_ID = new ResourceGroupId("test");
    private static final NodeInfo TEST_NODE_INFO = new NodeInfo("test");

    @Test
    public void testApplyDefaultProperties()
    {
        SessionPropertyDefaults sessionPropertyDefaults = new SessionPropertyDefaults(TEST_NODE_INFO, new SessionPropertyManager());
        SessionPropertyConfigurationManagerFactory factory = new TestingSessionPropertyConfigurationManagerFactory(
                ImmutableMap.<String, String>builder()
                        .put(QUERY_MAX_MEMORY, "5GB")
                        .put(EXECUTION_POLICY, "non_default_value")
                        .build(),
                ImmutableMap.of(
                        "testCatalog",
                        ImmutableMap.<String, String>builder()
                                .put("explicit_set", "override")
                                .put("catalog_default", "catalog_default")
                                .build()));
        sessionPropertyDefaults.addConfigurationManagerFactory(factory);
        sessionPropertyDefaults.setConfigurationManager(factory.getName(), ImmutableMap.of());

        Session session = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryId("test_query_id"))
                .setIdentity(new Identity("testUser", Optional.empty()))
                .setSystemProperty(QUERY_MAX_MEMORY, "1GB")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "43")
                .setCatalogSessionProperty("testCatalog", "explicit_set", "explicit_set")
                .build();

        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(HASH_PARTITION_COUNT, "43")
                .build());
        assertEquals(
                session.getUnprocessedCatalogProperties(),
                ImmutableMap.of(
                        "testCatalog",
                        ImmutableMap.<String, String>builder()
                                .put("explicit_set", "explicit_set")
                                .build()));

        session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, Optional.empty(), TEST_RESOURCE_GROUP_ID);

        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(HASH_PARTITION_COUNT, "43")
                .put(EXECUTION_POLICY, "non_default_value")
                .build());
        assertEquals(
                session.getUnprocessedCatalogProperties(),
                ImmutableMap.of(
                        "testCatalog",
                        ImmutableMap.<String, String>builder()
                                .put("explicit_set", "explicit_set")
                                .put("catalog_default", "catalog_default")
                                .build()));
    }

    @Test
    public void testBadSystemPropertyOverrides()
    {
        SessionPropertyDefaults sessionPropertyDefaults = new SessionPropertyDefaults(TEST_NODE_INFO, new SessionPropertyManager());
        SessionPropertyConfigurationManagerFactory factory = new TestingSessionPropertyConfigurationManagerFactory(
                ImmutableMap.<String, String>builder()
                        .put("not_a_system_property", "foo")
                        .put(HASH_PARTITION_COUNT, "not_an_integer")
                        .put(QUERY_MAX_MEMORY, "5000GB")
                        .build(),
                ImmutableMap.of());
        sessionPropertyDefaults.addConfigurationManagerFactory(factory);
        sessionPropertyDefaults.setConfigurationManager(factory.getName(), ImmutableMap.of());

        Session session = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryId("test_query_id"))
                .setIdentity(new Identity("testUser", Optional.empty()))
                .build();

        long failuresBefore = sessionPropertyDefaults.getSystemPropertyOverrideFailures().getTotalCount();
        session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, Optional.empty(), TEST_RESOURCE_GROUP_ID);
        long failuresAfter = sessionPropertyDefaults.getSystemPropertyOverrideFailures().getTotalCount();

        // Failures loading session properties not_a_system_property and HASH_PARTITION_COUNT
        assertEquals(failuresAfter - failuresBefore, 2);

        // Invalid session property value is ignored, takes the default value
        int hashPartitionsDefaultValue = (Integer) new SessionPropertyManager()
                .getSystemSessionPropertyMetadata(HASH_PARTITION_COUNT)
                .get()
                .getDefaultValue();
        assertEquals((int) session.getSystemProperty(HASH_PARTITION_COUNT, Integer.class), hashPartitionsDefaultValue);

        // Invalid session property name is dropped
        assertFalse(session.getSystemProperties().containsKey("not_a_system_property"));

        assertTrue(session.getSystemProperties().containsKey(QUERY_MAX_MEMORY));
        assertEquals(session.getSystemProperty(QUERY_MAX_MEMORY, DataSize.class), new DataSize(5000, GIGABYTE));
    }
}
