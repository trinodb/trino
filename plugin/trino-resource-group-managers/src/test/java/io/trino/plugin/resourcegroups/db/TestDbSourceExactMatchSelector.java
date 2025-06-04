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
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.spi.resourcegroups.QueryType.DELETE;
import static io.trino.spi.resourcegroups.QueryType.INSERT;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDbSourceExactMatchSelector
{
    private static final JsonCodec<ResourceGroupId> CODEC = JsonCodec.jsonCodec(ResourceGroupId.class);
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());
    private final H2ResourceGroupsDao dao;

    public TestDbSourceExactMatchSelector()
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_db-exact-match-selector" + System.nanoTime() + ThreadLocalRandom.current().nextLong());
        dao = new H2DaoProvider(config).get();
        dao.createExactMatchSelectorsTable();
    }

    @Test
    public void testMatch()
    {
        ResourceGroupId resourceGroupId1 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "insert"));
        ResourceGroupId resourceGroupId2 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "select"));
        dao.insertExactMatchSelector("test", "@test@test_pipeline", INSERT.name(), CODEC.toJson(resourceGroupId1));
        dao.insertExactMatchSelector("test", "@test@test_pipeline", SELECT.name(), CODEC.toJson(resourceGroupId2));

        DbSourceExactMatchSelector selector = new DbSourceExactMatchSelector("test", dao);

        assertThat(selector.match(new SelectionCriteria(true, "testuser", ImmutableSet.of(), "testuser", Optional.empty(), Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.empty()))).isEqualTo(Optional.empty());
        assertThat(selector.match(new SelectionCriteria(true, "testuser", ImmutableSet.of(), "testuser", Optional.empty(), Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.of(INSERT.name()))).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId1));
        assertThat(selector.match(new SelectionCriteria(true, "testuser", ImmutableSet.of(), "testuser", Optional.empty(), Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.of(SELECT.name()))).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId2));
        assertThat(selector.match(new SelectionCriteria(true, "testuser", ImmutableSet.of(), "testuser", Optional.empty(), Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.of(DELETE.name())))).isEqualTo(Optional.empty());

        assertThat(selector.match(new SelectionCriteria(true, "testuser", ImmutableSet.of(), "testuser", Optional.empty(), Optional.of("@test@test_new"), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of(INSERT.name())))).isEqualTo(Optional.empty());
    }
}
