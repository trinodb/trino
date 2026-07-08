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
package io.trino.plugin.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;

public final class ElasticsearchSessionProperties
        implements SessionPropertiesProvider
{
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    public static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";
    public static final String FULL_TEXT_PUSHDOWN_MODE = "full_text_pushdown_mode";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ElasticsearchSessionProperties(ElasticsearchConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        config.getDynamicFilteringWaitTimeout(),
                        false))
                .add(booleanProperty(
                        AGGREGATION_PUSHDOWN_ENABLED,
                        "Enable pushing down aggregations to Elasticsearch",
                        config.isAggregationPushdownEnabled(),
                        false))
                .add(enumProperty(
                        FULL_TEXT_PUSHDOWN_MODE,
                        "Push predicates and dynamic filters on analyzed text fields to Elasticsearch as full-text queries: DISABLED, SAFE, or UNSAFE",
                        FullTextPushdownMode.class,
                        config.getFullTextPushdownMode(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static boolean isAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static FullTextPushdownMode getFullTextPushdownMode(ConnectorSession session)
    {
        return session.getProperty(FULL_TEXT_PUSHDOWN_MODE, FullTextPushdownMode.class);
    }
}
