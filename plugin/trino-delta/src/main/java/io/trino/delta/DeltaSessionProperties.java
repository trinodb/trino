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
package io.trino.delta;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class DeltaSessionProperties
{
    public static final String PARTITION_PRUNING_ENABLED = "partition_pruning_enabled";
    public static final String FILTER_PUSHDOWN_ENABLED = "filter_pushdown_enabled";
    public static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DeltaSessionProperties(DeltaConfig deltaConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        PARTITION_PRUNING_ENABLED,
                        "Enable partition pruning in Delta tables",
                        deltaConfig.isPartitionPruningEnabled(),
                        false),
                booleanProperty(
                        FILTER_PUSHDOWN_ENABLED,
                        "Enable filter pushdown into Delta tables",
                        deltaConfig.isFilterPushdownEnabled(),
                        false),
                booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Enable project pushdown into Delta tables",
                        deltaConfig.isProjectionPushdownEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isPartitionPruningEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTITION_PRUNING_ENABLED, Boolean.class);
    }

    public static boolean isFilterPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(FILTER_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }
}
