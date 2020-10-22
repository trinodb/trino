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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;

public class JdbcMetadataSessionProperties
        implements SessionPropertiesProvider
{
    public static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public JdbcMetadataSessionProperties(JdbcMetadataConfig jdbcMetadataConfig)
    {
        properties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        AGGREGATION_PUSHDOWN_ENABLED,
                        "Enable aggregation pushdown",
                        jdbcMetadataConfig.isAggregationPushdownEnabled(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static boolean isAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }
}
