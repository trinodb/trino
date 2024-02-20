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
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class TypeHandlingJdbcSessionProperties
        implements SessionPropertiesProvider
{
    public static final String UNSUPPORTED_TYPE_HANDLING = "unsupported_type_handling";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public TypeHandlingJdbcSessionProperties(TypeHandlingJdbcConfig typeHandlingJdbcConfig)
    {
        properties = ImmutableList.of(
                enumProperty(
                        UNSUPPORTED_TYPE_HANDLING,
                        "Unsupported type handling strategy",
                        UnsupportedTypeHandling.class,
                        typeHandlingJdbcConfig.getUnsupportedTypeHandling(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static UnsupportedTypeHandling getUnsupportedTypeHandling(ConnectorSession session)
    {
        return session.getProperty(UNSUPPORTED_TYPE_HANDLING, UnsupportedTypeHandling.class);
    }
}
