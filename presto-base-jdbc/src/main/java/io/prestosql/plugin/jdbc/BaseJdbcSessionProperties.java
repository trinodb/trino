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

import static io.prestosql.spi.session.PropertyMetadata.enumProperty;

public final class BaseJdbcSessionProperties
        implements SessionPropertiesProvider
{
    public static final String UNSUPPORTED_TYPE_HANDLING_STRATEGY = "unsupported_type_handling_strategy";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public BaseJdbcSessionProperties(BaseJdbcConfig config)
    {
        sessionProperties = ImmutableList.of(
                enumProperty(
                        UNSUPPORTED_TYPE_HANDLING_STRATEGY,
                        "Configures how unsupported column data types should be handled",
                        UnsupportedTypeHandlingStrategy.class,
                        config.getUnsupportedTypeHandlingStrategy(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static UnsupportedTypeHandlingStrategy getUnsupportedTypeHandlingStrategy(ConnectorSession session)
    {
        return session.getProperty(UNSUPPORTED_TYPE_HANDLING_STRATEGY, UnsupportedTypeHandlingStrategy.class);
    }
}
