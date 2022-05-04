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
package io.trino.plugin.hidden.partitioning;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class HiddenPartitioningSessionProperties
        implements SessionPropertiesProvider
{
    private static final String PARTITION_SPEC_ENABLED = "partition_spec_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HiddenPartitioningSessionProperties(HiveSessionProperties hiveSessionProperties, HiddenPartitioningConfig config)
    {
        ImmutableList.Builder<PropertyMetadata<?>> builder = ImmutableList.builder();
        builder.addAll(hiveSessionProperties.getSessionProperties());
        builder.add(
                booleanProperty(
                        PARTITION_SPEC_ENABLED,
                        "Use partition spec if defined in table properties",
                        config.isPartitionSpecEnabled(),
                        false));
        sessionProperties = builder.build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isPartitionSpecEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTITION_SPEC_ENABLED, Boolean.class);
    }
}
