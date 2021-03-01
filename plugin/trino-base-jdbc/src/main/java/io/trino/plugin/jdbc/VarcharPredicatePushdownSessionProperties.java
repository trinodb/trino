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
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class VarcharPredicatePushdownSessionProperties
        implements SessionPropertiesProvider
{
    public static final String UNSAFE_VARCHAR_PUSHDOWN = "unsafe-varchar-pushdown";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public VarcharPredicatePushdownSessionProperties(VarcharPushdownConfig config)
    {
        properties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        UNSAFE_VARCHAR_PUSHDOWN,
                        "Enable potentially unsafe SQL pushdown based on char or varchar columns. " +
                                "Note that it may cause incorrect query results.",
                        config.isUnsafeVarcharPushdownEnabled(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static boolean isUnsafeVarcharPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(UNSAFE_VARCHAR_PUSHDOWN, Boolean.class);
    }
}
