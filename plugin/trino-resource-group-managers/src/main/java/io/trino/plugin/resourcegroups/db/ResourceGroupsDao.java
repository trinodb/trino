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
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;
import org.jdbi.v3.sqlobject.statement.UseRowReducer;

import java.util.List;

public interface ResourceGroupsDao
{
    @SqlUpdate(
            """
            CREATE TABLE IF NOT EXISTS resource_groups_global_properties (
              name VARCHAR(128) NOT NULL PRIMARY KEY,
              value VARCHAR(512) NULL,
              CHECK (name in ('cpu_quota_period', 'physical_data_scan_quota_period'))
            )
            """)
    void createResourceGroupsGlobalPropertiesTable();

    @SqlQuery("SELECT name, value FROM resource_groups_global_properties WHERE name IN ('cpu_quota_period', 'physical_data_scan_quota_period')")
    @UseRowReducer(ResourceGroupGlobalPropertiesReducer.class)
    ResourceGroupGlobalProperties getResourceGroupGlobalProperties();

    @SqlUpdate(
            """
            CREATE TABLE IF NOT EXISTS resource_groups (
              resource_group_id BIGINT NOT NULL AUTO_INCREMENT,
              name VARCHAR(250) NOT NULL,
              soft_memory_limit VARCHAR(128),
              max_queued INT NOT NULL,
              soft_concurrency_limit INT NULL,
              hard_concurrency_limit INT NOT NULL,
              scheduling_policy VARCHAR(128) NULL,
              scheduling_weight INT NULL,
              jmx_export BOOLEAN NULL,
              soft_cpu_limit VARCHAR(128) NULL,
              hard_cpu_limit VARCHAR(128) NULL,
              hard_physical_data_scan_limit VARCHAR(128) NULL,
              parent BIGINT NULL,
              environment VARCHAR(128) NULL,
              PRIMARY KEY (resource_group_id),
              FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id)
            )
            """)
    void createResourceGroupsTable();

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

    @SqlUpdate(
            """
            CREATE TABLE IF NOT EXISTS selectors (
              resource_group_id BIGINT NOT NULL,
              priority BIGINT NOT NULL,
              user_regex VARCHAR(512),
              user_group_regex VARCHAR(512),
              original_user_regex VARCHAR(512),
              authenticated_user_regex VARCHAR(512),
              source_regex VARCHAR(512),
              query_text_regex VARCHAR(1024),
              query_type VARCHAR(512),
              client_tags VARCHAR(512),
              selector_resource_estimate VARCHAR(1024),
              FOREIGN KEY (resource_group_id) REFERENCES resource_groups (resource_group_id)
            )
            """)
    void createSelectorsTable();

    @SqlUpdate(
            """
            CREATE TABLE IF NOT EXISTS exact_match_source_selectors(
              id BIGINT NOT NULL AUTO_INCREMENT,
              environment VARCHAR(128),
              source VARCHAR(512) NOT NULL,
              query_type VARCHAR(512),
              update_time TIMESTAMP NOT NULL,
              resource_group_id VARCHAR(256) NOT NULL,
              PRIMARY KEY (id)
            )
            """)
    void createExactMatchSelectorsTable();

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
