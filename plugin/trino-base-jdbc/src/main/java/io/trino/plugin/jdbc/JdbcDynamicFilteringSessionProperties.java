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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class JdbcDynamicFilteringSessionProperties
        implements SessionPropertiesProvider
{
    public static final String DYNAMIC_FILTERING_ENABLED = "dynamic_filtering_enabled";
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public JdbcDynamicFilteringSessionProperties(JdbcDynamicFilteringConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        DYNAMIC_FILTERING_ENABLED,
                        "Wait for dynamic filters before starting JDBC query",
                        config.isDynamicFilteringEnabled(),
                        false),
                durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters",
                        config.getDynamicFilteringWaitTimeout(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean dynamicFilteringEnabled(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_ENABLED, Boolean.class);
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }
}
