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
package io.trino.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.resourcegroups.ResourceGroupNameTemplate;
import io.trino.plugin.resourcegroups.SelectorResourceEstimate;
import io.trino.plugin.resourcegroups.SelectorResourceEstimate.Range;
import io.trino.spi.resourcegroups.ResourceGroupId;
import org.h2.jdbc.JdbcException;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.trino.spi.resourcegroups.QueryType.DELETE;
import static io.trino.spi.resourcegroups.QueryType.EXPLAIN;
import static io.trino.spi.resourcegroups.QueryType.INSERT;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestResourceGroupsDao
{
    private static final String ENVIRONMENT = "test";
    private static final SelectorResourceEstimate SELECTOR_RESOURCE_ESTIMATE = new SelectorResourceEstimate(
            Optional.of(new Range<>(
                    Optional.empty(),
                    Optional.of(new Duration(5, MINUTES)))),
            Optional.of(new SelectorResourceEstimate.Range<>(
                    Optional.of(new Duration(10, SECONDS)),
                    Optional.empty())),
            Optional.of(new Range<>(
                    Optional.empty(),
                    Optional.of(DataSize.valueOf("500MB")))));

    private static final JsonCodec<List<String>> LIST_STRING_CODEC = listJsonCodec(String.class);
    private static final JsonCodec<SelectorResourceEstimate> SELECTOR_RESOURCE_ESTIMATE_JSON_CODEC = jsonCodec(SelectorResourceEstimate.class);

    static H2ResourceGroupsDao setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime() + ThreadLocalRandom.current().nextLong() + ";NON_KEYWORDS=KEY,VALUE"); // key and value are reserved keywords in H2 2.x
        return new H2DaoProvider(config).get();
    }

    @Test
    public void testResourceGroups()
    {
        H2ResourceGroupsDao dao = setup("resource_groups");
        dao.createResourceGroupsTable();
        Map<Long, ResourceGroupSpecBuilder> map = new HashMap<>();
        testResourceGroupInsert(dao, map);
        testResourceGroupUpdate(dao, map);
        testResourceGroupDelete(dao, map);
    }

    private static void testResourceGroupInsert(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "bi", "50%", 50, 50, 50, null, null, null, null, null, 1L, ENVIRONMENT);
        List<ResourceGroupSpecBuilder> records = dao.getResourceGroups(ENVIRONMENT);
        assertThat(records).hasSize(2);
        map.put(1L, new ResourceGroupSpecBuilder(1, new ResourceGroupNameTemplate("global"), Optional.of("100%"), 100, Optional.of(100), 100, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        map.put(2L, new ResourceGroupSpecBuilder(2, new ResourceGroupNameTemplate("bi"), Optional.of("50%"), 50, Optional.of(50), 50, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(1L)));
        compareResourceGroups(map, records);
    }

    private static void testResourceGroupUpdate(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        dao.updateResourceGroup(2, "bi", null, 40, 30, 30, null, null, true, null, null, 1L, ENVIRONMENT);
        ResourceGroupSpecBuilder updated = new ResourceGroupSpecBuilder(2, new ResourceGroupNameTemplate("bi"), Optional.empty(), 40, Optional.of(30), 30, Optional.empty(), Optional.empty(), Optional.of(true), Optional.empty(), Optional.empty(), Optional.of(1L));
        map.put(2L, updated);
        compareResourceGroups(map, dao.getResourceGroups(ENVIRONMENT));
    }

    private static void testResourceGroupDelete(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        dao.deleteResourceGroup(2);
        map.remove(2L);
        compareResourceGroups(map, dao.getResourceGroups(ENVIRONMENT));
    }

    @Test
    public void testSelectors()
    {
        H2ResourceGroupsDao dao = setup("selectors");
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        Map<Long, SelectorRecord> map = new HashMap<>();
        testSelectorInsert(dao, map);
        testSelectorUpdate(dao, map);
        testSelectorUpdateNull(dao, map);
        testSelectorDelete(dao, map);
        testSelectorDeleteNull(dao, map);
        testSelectorMultiDelete(dao, map);
    }

    private static void testSelectorInsert(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        map.put(2L,
                new SelectorRecord(
                        2L,
                        1L,
                        Optional.of(Pattern.compile("ping_user")),
                        Optional.of(Pattern.compile("ping_group")),
                        Optional.of(Pattern.compile("ping_original_user")),
                        Optional.of(Pattern.compile("ping_auth_user")),
                        Optional.of(Pattern.compile(".*")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
        map.put(3L,
                new SelectorRecord(
                        3L,
                        2L,
                        Optional.of(Pattern.compile("admin_user")),
                        Optional.of(Pattern.compile("admin_group")),
                        Optional.of(Pattern.compile("admin_original_user")),
                        Optional.of(Pattern.compile("admin_auth_user")),
                        Optional.of(Pattern.compile(".*")),
                        Optional.of(EXPLAIN.name()),
                        Optional.of(ImmutableList.of("tag1", "tag2")),
                        Optional.empty()));
        map.put(4L,
                new SelectorRecord(
                        4L,
                        0L,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(SELECTOR_RESOURCE_ESTIMATE)));

        dao.insertResourceGroup(1, "admin", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "ping_query", "50%", 50, 50, 50, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroup(3, "config", "50%", 50, 50, 50, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroup(4, "config", "50%", 50, 50, 50, null, null, null, null, null, 1L, ENVIRONMENT);

        dao.insertSelector(2, 1, "ping_user", "ping_group", "ping_original_user", "ping_auth_user", ".*", null, null, null);
        dao.insertSelector(3, 2, "admin_user", "admin_group", "admin_original_user", "admin_auth_user", ".*", EXPLAIN.name(), LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")), null);
        dao.insertSelector(4, 0, null, null, null, null, null, null, null, SELECTOR_RESOURCE_ESTIMATE_JSON_CODEC.toJson(SELECTOR_RESOURCE_ESTIMATE));
        List<SelectorRecord> records = dao.getSelectors(ENVIRONMENT);
        compareSelectors(map, records);
    }

    private static void testSelectorUpdate(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        dao.updateSelector(2, "ping.*", "ping_gr.*", "ping_original.*", "ping_auth.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1")), "ping_user", "ping_group", "ping_original_user", "ping_auth_user", ".*", null);
        SelectorRecord updated = new SelectorRecord(
                2,
                1L,
                Optional.of(Pattern.compile("ping.*")),
                Optional.of(Pattern.compile("ping_gr.*")),
                Optional.of(Pattern.compile("ping_original.*")),
                Optional.of(Pattern.compile("ping_auth.*")),
                Optional.of(Pattern.compile("ping_source")),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1")),
                Optional.empty());
        map.put(2L, updated);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorUpdateNull(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        SelectorRecord updated = new SelectorRecord(2, 3L, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        map.put(2L, updated);
        dao.updateSelector(2, null, null, null, null, null, null, "ping.*", "ping_gr.*", "ping_original.*", "ping_auth.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1")));
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
        updated = new SelectorRecord(
                2,
                2L,
                Optional.of(Pattern.compile("ping.*")),
                Optional.of(Pattern.compile("ping_gr.*")),
                Optional.of(Pattern.compile("ping_original.*")),
                Optional.of(Pattern.compile("ping_auth.*")),
                Optional.of(Pattern.compile("ping_source")),
                Optional.of(EXPLAIN.name()),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty());
        map.put(2L, updated);
        dao.updateSelector(2, "ping.*", "ping_gr.*", "ping_original.*", "ping_auth.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")), null, null, null, null, null, null);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorDelete(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        map.remove(2L);
        dao.deleteSelector(2, "ping.*", "ping_gr.*", "ping_original.*", "ping_auth.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorDeleteNull(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        dao.updateSelector(3, null, null, null, null, null, null, "admin_user", "admin_group", "admin_original_user", "admin_auth_user", ".*", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        SelectorRecord nullRegexes = new SelectorRecord(3L, 2L, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        map.put(3L, nullRegexes);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
        dao.deleteSelector(3, null, null, null, null, null, null);
        map.remove(3L);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    private static void testSelectorMultiDelete(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        if (dao != null) {
            return;
        }

        dao.insertSelector(3, 3L, "user1", null, null, null, "pipeline", null, null, null);
        map.put(3L, new SelectorRecord(
                3L,
                3L,
                Optional.of(Pattern.compile("user1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("pipeline")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
        dao.deleteSelectors(3L);
        map.remove(3L);
        compareSelectors(map, dao.getSelectors(ENVIRONMENT));
    }

    @Test
    public void testGlobalResourceGroupProperties()
    {
        H2ResourceGroupsDao dao = setup("global_properties");
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        ResourceGroupGlobalProperties globalProperties = new ResourceGroupGlobalProperties(Optional.of(new Duration(1, HOURS)));
        ResourceGroupGlobalProperties records = dao.getResourceGroupGlobalProperties().get(0);
        assertThat(globalProperties).isEqualTo(records);
        try {
            dao.insertResourceGroupsGlobalProperties("invalid_property", "1h");
        }
        catch (UnableToExecuteStatementException ex) {
            assertThat(ex.getCause()).isInstanceOf(JdbcException.class);
            assertThat(ex.getCause().getMessage()).startsWith("Check constraint violation:");
        }
        try {
            dao.updateResourceGroupsGlobalProperties("invalid_property_name");
        }
        catch (UnableToExecuteStatementException ex) {
            assertThat(ex.getCause()).isInstanceOf(JdbcException.class);
            assertThat(ex.getCause().getMessage()).startsWith("Check constraint violation:");
        }
    }

    @Test
    public void testExactMatchSelector()
    {
        H2ResourceGroupsDao dao = setup("exact_match_selector");
        dao.createExactMatchSelectorsTable();

        ResourceGroupId resourceGroupId1 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "insert"));
        ResourceGroupId resourceGroupId2 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "select"));
        JsonCodec<ResourceGroupId> codec = JsonCodec.jsonCodec(ResourceGroupId.class);
        dao.insertExactMatchSelector("test", "@test@test_pipeline", INSERT.name(), codec.toJson(resourceGroupId1));
        dao.insertExactMatchSelector("test", "@test@test_pipeline", SELECT.name(), codec.toJson(resourceGroupId2));

        assertThat(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", null)).isNull();
        assertThat(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", INSERT.name())).isEqualTo(codec.toJson(resourceGroupId1));
        assertThat(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", SELECT.name())).isEqualTo(codec.toJson(resourceGroupId2));
        assertThat(dao.getExactMatchResourceGroup("test", "@test@test_pipeline", DELETE.name())).isNull();

        assertThat(dao.getExactMatchResourceGroup("test", "abc", INSERT.name())).isNull();
        assertThat(dao.getExactMatchResourceGroup("prod", "@test@test_pipeline", INSERT.name())).isNull();
    }

    private static void compareResourceGroups(Map<Long, ResourceGroupSpecBuilder> map, List<ResourceGroupSpecBuilder> records)
    {
        assertThat(map).hasSize(records.size());
        for (ResourceGroupSpecBuilder record : records) {
            ResourceGroupSpecBuilder expected = map.get(record.getId());
            assertThat(record.build()).isEqualTo(expected.build());
        }
    }

    private static void compareSelectors(Map<Long, SelectorRecord> map, List<SelectorRecord> records)
    {
        assertThat(map).hasSize(records.size());
        for (SelectorRecord record : records) {
            SelectorRecord expected = map.get(record.getResourceGroupId());
            assertThat(record.getResourceGroupId()).isEqualTo(expected.getResourceGroupId());
            assertThat(record.getUserRegex().map(Pattern::pattern)).isEqualTo(expected.getUserRegex().map(Pattern::pattern));
            assertThat(record.getUserGroupRegex().map(Pattern::pattern)).isEqualTo(expected.getUserGroupRegex().map(Pattern::pattern));
            assertThat(record.getSourceRegex().map(Pattern::pattern)).isEqualTo(expected.getSourceRegex().map(Pattern::pattern));
            assertThat(record.getOriginalUserRegex().map(Pattern::pattern)).isEqualTo(expected.getOriginalUserRegex().map(Pattern::pattern));
            assertThat(record.getAuthenticatedUserRegex().map(Pattern::pattern)).isEqualTo(expected.getAuthenticatedUserRegex().map(Pattern::pattern));
            assertThat(record.getSelectorResourceEstimate()).isEqualTo(expected.getSelectorResourceEstimate());
        }
    }
}
