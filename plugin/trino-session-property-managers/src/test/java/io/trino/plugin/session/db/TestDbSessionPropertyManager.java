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
package io.trino.plugin.session.db;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.session.AbstractTestSessionPropertyManager;
import io.trino.plugin.session.SessionMatchSpec;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.SessionConfigurationContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestDbSessionPropertyManager
        extends AbstractTestSessionPropertyManager
{
    private DbSessionPropertyManagerConfig config;
    private SessionPropertiesDao dao;
    private DbSessionPropertyManager manager;
    private RefreshingDbSpecsProvider specsProvider;

    private TestingMySqlContainer mysqlContainer;

    private static final ResourceGroupId TEST_RG = new ResourceGroupId("rg1");

    @BeforeClass
    public void setup()
    {
        mysqlContainer = new TestingMySqlContainer();
        mysqlContainer.start();

        config = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl(mysqlContainer.getJdbcUrl())
                .setUsername(mysqlContainer.getUsername())
                .setPassword(mysqlContainer.getPassword());

        SessionPropertiesDaoProvider daoProvider = new SessionPropertiesDaoProvider(config);
        dao = daoProvider.get();
    }

    @BeforeMethod
    public void setupTest()
    {
        specsProvider = new RefreshingDbSpecsProvider(config, dao);
        manager = new DbSessionPropertyManager(specsProvider);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        dao.dropSessionPropertiesTable();
        dao.dropSessionClientTagsTable();
        dao.dropSessionSpecsTable();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        specsProvider.destroy();
        mysqlContainer.close();
        mysqlContainer = null;
    }

    @Override
    protected void assertProperties(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties, SessionMatchSpec... specs)
    {
        insertSpecs(specs);
        long failureCountBefore = specsProvider.getDbLoadFailures().getTotalCount();
        specsProvider.refresh();
        long failureCountAfter = specsProvider.getDbLoadFailures().getTotalCount();
        assertEquals(failureCountAfter, failureCountBefore, "specs refresh should not fail");
        assertEquals(manager.getSystemSessionProperties(CONTEXT), systemProperties);
        assertEquals(manager.getCatalogSessionProperties(CONTEXT), catalogProperties);
    }

    private void insertSpecs(SessionMatchSpec[] specs)
    {
        for (int i = 1; i <= specs.length; i++) {
            SessionMatchSpec spec = specs[i - 1];
            String userRegex = spec.getUserRegex().map(Pattern::pattern).orElse(null);
            String sourceRegex = spec.getSourceRegex().map(Pattern::pattern).orElse(null);
            String queryType = spec.getQueryType().orElse(null);
            String resourceGroupRegex = spec.getResourceGroupRegex().map(Pattern::pattern).orElse(null);

            dao.insertSpecRow(i, userRegex, sourceRegex, queryType, resourceGroupRegex, 0);

            for (String tag : spec.getClientTags()) {
                dao.insertClientTag(i, tag);
            }

            int propertyId = i;
            spec.getSessionProperties().forEach((key, value) -> dao.insertSessionProperty(propertyId, key, value));
        }
    }

    /**
     * A basic test for session property overrides with the {@link DbSessionPropertyManager}
     */
    @Test
    public void testSessionProperties()
    {
        dao.insertSpecRow(1, "foo.*", null, null, null, 0);
        dao.insertSessionProperty(1, "prop_1", "val_1");

        dao.insertSpecRow(2, ".*", "bar.*", null, null, 0);
        dao.insertSessionProperty(2, "prop_2", "val_2");

        specsProvider.refresh();
        SessionConfigurationContext context1 = new SessionConfigurationContext("foo123", Optional.of("src1"),
                ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties1 = manager.getSystemSessionProperties(context1);
        assertEquals(sessionProperties1.get("prop_1"), "val_1");
        assertFalse(sessionProperties1.containsKey("prop_2"));

        specsProvider.refresh();
        SessionConfigurationContext context2 = new SessionConfigurationContext("bar123", Optional.of("bar123"),
                ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties2 = manager.getSystemSessionProperties(context2);
        assertEquals(sessionProperties2.get("prop_2"), "val_2");
        assertFalse(sessionProperties2.containsKey("prop_1"));

        specsProvider.refresh();
        SessionConfigurationContext context3 = new SessionConfigurationContext("foo123", Optional.of("bar123"),
                ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties3 = manager.getSystemSessionProperties(context3);
        assertEquals(sessionProperties3.get("prop_1"), "val_1");
        assertEquals(sessionProperties3.get("prop_2"), "val_2");

        specsProvider.refresh();
        SessionConfigurationContext context4 = new SessionConfigurationContext("abc", Optional.empty(), ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties4 = manager.getSystemSessionProperties(context4);
        assertFalse(sessionProperties4.containsKey("prop_1"));
        assertFalse(sessionProperties4.containsKey("prop_2"));
    }

    /**
     * Test {@link DbSessionPropertyManager#getSystemSessionProperties} after unsuccessful reloading
     */
    @Test
    public void testReloads()
    {
        SessionConfigurationContext context1 = new SessionConfigurationContext("foo123", Optional.of("src1"), ImmutableSet.of(), Optional.empty(), TEST_RG);

        dao.insertSpecRow(1, "foo.*", null, null, null, 0);
        dao.insertSessionProperty(1, "prop_1", "val_1");
        dao.insertSpecRow(2, ".*", "bar.*", null, null, 0);
        dao.insertSessionProperty(2, "prop_2", "val_2");

        specsProvider.refresh();
        long failuresBefore = specsProvider.getDbLoadFailures().getTotalCount();

        dao.insertSpecRow(3, "bar", null, null, null, 0);
        dao.insertSessionProperty(3, "prop_3", "val_3");

        // Simulating bad database operation
        dao.dropSessionPropertiesTable();

        specsProvider.refresh();
        long failuresAfter = specsProvider.getDbLoadFailures().getTotalCount();

        // Failed reloading, use cached configurations
        assertEquals(failuresAfter - failuresBefore, 1);
        Map<String, String> sessionProperties1 = manager.getSystemSessionProperties(context1);
        assertEquals(sessionProperties1.get("prop_1"), "val_1");
        assertEquals(sessionProperties1.get("prop_3"), null);
    }

    /**
     * A test for conflicting values for the same session property
     */
    @Test
    public void testOrderingOfSpecs()
    {
        dao.insertSpecRow(1, "foo", null, null, null, 2);
        dao.insertSessionProperty(1, "prop_1", "val_1_2");
        dao.insertSessionProperty(1, "prop_2", "val_2_2");

        dao.insertSpecRow(2, "foo", null, null, null, 1);
        dao.insertSessionProperty(2, "prop_1", "val_1_1");
        dao.insertSessionProperty(2, "prop_2", "val_2_1");
        dao.insertSessionProperty(2, "prop_3", "val_3_1");

        dao.insertSpecRow(3, "foo", null, null, null, 3);
        dao.insertSessionProperty(3, "prop_1", "val_1_3");

        specsProvider.refresh();

        SessionConfigurationContext context = new SessionConfigurationContext("foo", Optional.of("bar"), ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties = manager.getSystemSessionProperties(context);
        assertEquals(sessionProperties.get("prop_1"), "val_1_3");
        assertEquals(sessionProperties.get("prop_2"), "val_2_2");
        assertEquals(sessionProperties.get("prop_3"), "val_3_1");
        assertEquals(sessionProperties.size(), 3);
    }

    @Test
    public void testCatalogSessionProperties()
    {
        dao.insertSpecRow(1, ".*", null, null, null, 0);
        dao.insertSessionProperty(1, "catalog_1.prop_1", "val_1");
        dao.insertSessionProperty(1, "catalog_1.prop_2", "val_2");

        dao.insertSpecRow(2, ".*", null, null, null, 1);
        dao.insertSessionProperty(2, "catalog_1.prop_1", "val_1_bis");
        dao.insertSessionProperty(2, "catalog_1.prop_3", "val_3");

        specsProvider.refresh();
        SessionConfigurationContext context1 = new SessionConfigurationContext("foo", Optional.empty(), ImmutableSet.of(), Optional.empty(), TEST_RG);
        assertEquals(manager.getCatalogSessionProperties(context1),
                ImmutableMap.of("catalog_1",
                        ImmutableMap.of("prop_1", "val_1_bis", "prop_2", "val_2", "prop_3", "val_3")));
    }

    @Test
    public void testEmptyTables()
    {
        specsProvider.refresh();
        SessionConfigurationContext context1 = new SessionConfigurationContext("foo", Optional.empty(), ImmutableSet.of(), Optional.empty(), TEST_RG);
        assertEquals(manager.getSystemSessionProperties(context1), ImmutableMap.of());
        assertEquals(manager.getCatalogSessionProperties(context1), ImmutableMap.of());
    }
}
