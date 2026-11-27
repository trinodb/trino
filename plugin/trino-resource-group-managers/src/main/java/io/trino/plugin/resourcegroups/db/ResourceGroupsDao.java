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

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;
import org.jdbi.v3.sqlobject.statement.UseRowReducer;

import java.util.List;

public interface ResourceGroupsDao
{
    @SqlQuery("SELECT name, value FROM resource_groups_global_properties WHERE name IN ('cpu_quota_period', 'physical_data_scan_quota_period')")
    @UseRowReducer(ResourceGroupGlobalPropertiesReducer.class)
    ResourceGroupGlobalProperties getResourceGroupGlobalProperties();

    @SqlQuery(
            """
            SELECT resource_group_id, name, soft_memory_limit, max_queued, soft_concurrency_limit,
              hard_concurrency_limit, scheduling_policy, scheduling_weight, jmx_export, soft_cpu_limit,
              hard_cpu_limit, hard_physical_data_scan_limit, parent
            FROM resource_groups
            WHERE environment = :environment OR environment IS NULL
            """)
    @UseRowMapper(ResourceGroupSpecBuilder.Mapper.class)
    List<ResourceGroupSpecBuilder> getResourceGroups(@Bind("environment") String environment);

    @SqlQuery(
            """
            SELECT S.resource_group_id, S.priority, S.user_regex, S.source_regex, S.original_user_regex, S.authenticated_user_regex, S.query_text_regex, S.query_type, S.client_tags, S.selector_resource_estimate, S.user_group_regex
            FROM selectors S
            JOIN resource_groups R ON (S.resource_group_id = R.resource_group_id)
            WHERE (R.environment = :environment OR R.environment IS NULL)
            ORDER by priority DESC
            """)
    @UseRowMapper(SelectorRecord.Mapper.class)
    List<SelectorRecord> getSelectors(@Bind("environment") String environment);

    /**
     * Returns the most specific exact-match selector for a given environment, source and query type.
     * NULL values in the environment and query type fields signify wildcards.
     */
    @SqlQuery(
            """
            SELECT resource_group_id
            FROM exact_match_source_selectors
            WHERE source = :source
              AND (environment = :environment OR environment IS NULL)
              AND (query_type = :query_type OR query_type IS NULL)
            ORDER BY environment IS NULL, query_type IS NULL
            LIMIT 1
            """)
    String getExactMatchResourceGroup(
            @Bind("environment") String environment,
            @Bind("source") String source,
            @Bind("query_type") String queryType);
}
