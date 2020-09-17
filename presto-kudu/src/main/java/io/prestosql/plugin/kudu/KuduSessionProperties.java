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
package io.prestosql.plugin.kudu;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;

public final class KuduSessionProperties
{
    private static final String KUDU_GROUPED_EXECUTION_ENABLED = "grouped_execution";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KuduSessionProperties(KuduClientConfig kuduConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
            .add(booleanProperty(
                KUDU_GROUPED_EXECUTION_ENABLED,
                "Enable grouped execution.",
                kuduConfig.isGroupedExecutionEnabled(),
                false))
            .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isKuduGroupedExecutionEnabled(ConnectorSession session)
    {
        return session.getProperty(KUDU_GROUPED_EXECUTION_ENABLED, Boolean.class);
    }
}
