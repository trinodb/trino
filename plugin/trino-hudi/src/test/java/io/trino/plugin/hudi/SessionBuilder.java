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
package io.trino.plugin.hudi;

import io.trino.Session;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.plugin.hudi.HudiSessionProperties.COLUMN_STATS_INDEX_ENABLED;
import static io.trino.plugin.hudi.HudiSessionProperties.COLUMN_STATS_WAIT_TIMEOUT;
import static io.trino.plugin.hudi.HudiSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.plugin.hudi.HudiSessionProperties.METADATA_TABLE_ENABLED;
import static io.trino.plugin.hudi.HudiSessionProperties.PARTITION_STATS_INDEX_ENABLED;
import static io.trino.plugin.hudi.HudiSessionProperties.QUERY_PARTITION_FILTER_REQUIRED;
import static io.trino.plugin.hudi.HudiSessionProperties.RECORD_INDEX_WAIT_TIMEOUT;
import static io.trino.plugin.hudi.HudiSessionProperties.RECORD_LEVEL_INDEX_ENABLED;
import static io.trino.plugin.hudi.HudiSessionProperties.SECONDARY_INDEX_ENABLED;
import static io.trino.plugin.hudi.HudiSessionProperties.SECONDARY_INDEX_WAIT_TIMEOUT;
import static io.trino.plugin.hudi.HudiSessionProperties.TABLE_STATISTICS_ENABLED;
import static java.util.Objects.requireNonNull;

public class SessionBuilder
{
    private final Session.SessionBuilder sessionBuilder;
    private final String catalogName;

    private SessionBuilder(Session session)
    {
        requireNonNull(session, "Initial session cannot be null");
        this.sessionBuilder = Session.builder(session);
        this.catalogName = session.getCatalog()
                .orElseThrow(() -> new IllegalStateException("Session must have a catalog to configure properties."));
    }

    /**
     * Creates a new SessionPropertyConfigurator from an existing session.
     *
     * @param session The base session to build upon.
     * @return A new instance of SessionPropertyConfigurator.
     */
    public static SessionBuilder from(Session session)
    {
        return new SessionBuilder(session);
    }

    private SessionBuilder setCatalogProperty(String propertyName, String propertyValue)
    {
        this.sessionBuilder.setCatalogSessionProperty(catalogName, propertyName, propertyValue);
        return this;
    }

    private SessionBuilder setSystemProperty(String propertyName, String propertyValue)
    {
        this.sessionBuilder.setSystemProperty(propertyName, propertyValue);
        return this;
    }

    /**
     * Builds the new Session with the configured properties.
     *
     * @return The newly configured Session object.
     */
    public Session build()
    {
        return this.sessionBuilder.build();
    }

    public SessionBuilder withJoinDistributionType(String joinDistributionType)
    {
        return setSystemProperty(JOIN_DISTRIBUTION_TYPE, joinDistributionType);
    }

    public SessionBuilder withPartitionFilterRequired(boolean required)
    {
        return setCatalogProperty(QUERY_PARTITION_FILTER_REQUIRED, String.valueOf(required));
    }

    public SessionBuilder withTableStatisticsEnabled(boolean enabled)
    {
        return setCatalogProperty(TABLE_STATISTICS_ENABLED, String.valueOf(enabled));
    }

    public SessionBuilder withMdtEnabled(boolean enabled)
    {
        return setCatalogProperty(METADATA_TABLE_ENABLED, String.valueOf(enabled));
    }

    public SessionBuilder withDynamicFilterEnabled(boolean isDynamicFilterEnabled)
    {
        return setSystemProperty(ENABLE_DYNAMIC_FILTERING, String.valueOf(isDynamicFilterEnabled));
    }

    public SessionBuilder withDynamicFilterTimeout(String durationProp)
    {
        return setCatalogProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, durationProp);
    }

    public SessionBuilder withColStatsIndexEnabled(boolean enabled)
    {
        return setCatalogProperty(COLUMN_STATS_INDEX_ENABLED, String.valueOf(enabled));
    }

    public SessionBuilder withRecordLevelIndexEnabled(boolean enabled)
    {
        return setCatalogProperty(RECORD_LEVEL_INDEX_ENABLED, String.valueOf(enabled));
    }

    public SessionBuilder withSecondaryIndexEnabled(boolean enabled)
    {
        return setCatalogProperty(SECONDARY_INDEX_ENABLED, String.valueOf(enabled));
    }

    public SessionBuilder withPartitionStatsIndexEnabled(boolean enabled)
    {
        return setCatalogProperty(PARTITION_STATS_INDEX_ENABLED, String.valueOf(enabled));
    }

    public SessionBuilder withColumnStatsTimeout(String durationProp)
    {
        return setCatalogProperty(COLUMN_STATS_WAIT_TIMEOUT, durationProp);
    }

    public SessionBuilder withSecondaryIndexTimeout(String durationProp)
    {
        return setCatalogProperty(SECONDARY_INDEX_WAIT_TIMEOUT, durationProp);
    }

    public SessionBuilder withRecordIndexTimeout(String durationProp)
    {
        return setCatalogProperty(RECORD_INDEX_WAIT_TIMEOUT, durationProp);
    }
}
