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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;

public final class PrometheusSessionProperties
        implements SessionPropertiesProvider
{
    private static final String QUERY_CHUNK_SIZE_DURATION = "query_chunk_size_duration";
    private static final String MAX_QUERY_RANGE_DURATION = "max_query_range_duration";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PrometheusSessionProperties(PrometheusConnectorConfig connectorConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(durationProperty(
                        QUERY_CHUNK_SIZE_DURATION,
                        "The duration of each query to Prometheus",
                        connectorConfig.getQueryChunkSizeDuration(),
                        false))
                .add(durationProperty(
                        MAX_QUERY_RANGE_DURATION,
                        "Width of overall query to Prometheus, will be divided into query_chunk_size_duration queries",
                        connectorConfig.getMaxQueryRangeDuration(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Duration getQueryChunkSize(ConnectorSession session)
    {
        return session.getProperty(QUERY_CHUNK_SIZE_DURATION, Duration.class);
    }

    public static Duration getMaxQueryRange(ConnectorSession session)
    {
        return session.getProperty(MAX_QUERY_RANGE_DURATION, Duration.class);
    }
}
