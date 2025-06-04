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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDbSessionPropertyManager
        extends AbstractTestSessionPropertyManager
{
    private DbSessionPropertyManagerConfig config;
    private SessionPropertiesDao dao;
    private DbSessionPropertyManager manager;
    private RefreshingDbSpecsProvider specsProvider;

    private TestingMySqlContainer mysqlContainer;

    private static final ResourceGroupId TEST_RG = new ResourceGroupId("rg1");

    @BeforeAll
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

    @BeforeEach
    public void setupTest()
    {
        specsProvider = new RefreshingDbSpecsProvider(config, dao);
        manager = new DbSessionPropertyManager(specsProvider);
    }

    @AfterEach
    public void teardown()
    {
        dao.dropSessionPropertiesTable();
        dao.dropSessionClientTagsTable();
        dao.dropSessionSpecsTable();
    }

    @AfterAll
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
        assertThat(failureCountAfter)
                .describedAs("specs refresh should not fail")
                .isEqualTo(failureCountBefore);
        assertThat(manager.getSystemSessionProperties(CONTEXT)).isEqualTo(systemProperties);
        assertThat(manager.getCatalogSessionProperties(CONTEXT)).isEqualTo(catalogProperties);
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
        assertThat(sessionProperties1)
                .containsEntry("prop_1", "val_1")
                .doesNotContainKey("prop_2");

        specsProvider.refresh();
        SessionConfigurationContext context2 = new SessionConfigurationContext("bar123", Optional.of("bar123"),
                ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties2 = manager.getSystemSessionProperties(context2);
        assertThat(sessionProperties2)
                .doesNotContainKey("prop_1")
                .containsEntry("prop_2", "val_2");

        specsProvider.refresh();
        SessionConfigurationContext context3 = new SessionConfigurationContext("foo123", Optional.of("bar123"),
                ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties3 = manager.getSystemSessionProperties(context3);
        assertThat(sessionProperties3)
                .containsEntry("prop_1", "val_1")
                .containsEntry("prop_2", "val_2");

        specsProvider.refresh();
        SessionConfigurationContext context4 = new SessionConfigurationContext("abc", Optional.empty(), ImmutableSet.of(), Optional.empty(), TEST_RG);
        Map<String, String> sessionProperties4 = manager.getSystemSessionProperties(context4);
        assertThat(sessionProperties4)
                .doesNotContainKey("prop_1")
                .doesNotContainKey("prop_2");
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
        assertThat(failuresAfter - failuresBefore).isEqualTo(1);
        Map<String, String> sessionProperties1 = manager.getSystemSessionProperties(context1);
        assertThat(sessionProperties1).containsEntry("prop_1", "val_1");
        assertThat(sessionProperties1).doesNotContainKey("prop_3");
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
        assertThat(sessionProperties).containsEntry("prop_1", "val_1_3");
        assertThat(sessionProperties).containsEntry("prop_2", "val_2_2");
        assertThat(sessionProperties).containsEntry("prop_3", "val_3_1");
        assertThat(sessionProperties).hasSize(3);
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
        assertThat(manager.getCatalogSessionProperties(context1)).isEqualTo(ImmutableMap.of("catalog_1",
                ImmutableMap.of("prop_1", "val_1_bis", "prop_2", "val_2", "prop_3", "val_3")));
    }

    @Test
    public void testEmptyTables()
    {
        specsProvider.refresh();
        SessionConfigurationContext context1 = new SessionConfigurationContext("foo", Optional.empty(), ImmutableSet.of(), Optional.empty(), TEST_RG);
        assertThat(manager.getSystemSessionProperties(context1)).isEqualTo(ImmutableMap.of());
        assertThat(manager.getCatalogSessionProperties(context1)).isEqualTo(ImmutableMap.of());
    }
}
