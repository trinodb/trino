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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class DruidSessionProperties
        implements SessionPropertiesProvider
{
    public static final String USE_DEFAULT_VALUE_FOR_NULL = "use_default_value_for_null";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DruidSessionProperties(DruidConfig druidConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        USE_DEFAULT_VALUE_FOR_NULL,
                        "Druid cluster runs with druid.generic.useDefaultValueForNull=true (legacy null handling); disables aggregation pushdown to preserve Trino NULL semantics",
                        druidConfig.isUseDefaultValueForNull(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isUseDefaultValueForNull(ConnectorSession session)
    {
        return session.getProperty(USE_DEFAULT_VALUE_FOR_NULL, Boolean.class);
    }
}
