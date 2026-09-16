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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class IcebergRestSessionProperties
        implements SessionPropertiesProvider
{
    private static final String CASE_INSENSITIVE_NAME_MATCHING_TABLES = "case_insensitive_name_matching_tables";
    private static final String CASE_INSENSITIVE_NAME_MATCHING_NAMESPACES = "case_insensitive_name_matching_namespaces";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public IcebergRestSessionProperties(IcebergRestCatalogConfig restConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        CASE_INSENSITIVE_NAME_MATCHING_TABLES,
                        "Match Iceberg REST table and view names case insensitively",
                        restConfig.getCaseInsensitiveNameMatchingForTables().orElse(restConfig.isCaseInsensitiveNameMatching()),
                        false))
                .add(booleanProperty(
                        CASE_INSENSITIVE_NAME_MATCHING_NAMESPACES,
                        "Match Iceberg REST namespace names case insensitively",
                        restConfig.getCaseInsensitiveNameMatchingForNamespaces().orElse(restConfig.isCaseInsensitiveNameMatching()),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isCaseInsensitiveNameMatchingForTables(ConnectorSession session)
    {
        return session.getProperty(CASE_INSENSITIVE_NAME_MATCHING_TABLES, Boolean.class);
    }

    public static boolean isCaseInsensitiveNameMatchingForNamespaces(ConnectorSession session)
    {
        return session.getProperty(CASE_INSENSITIVE_NAME_MATCHING_NAMESPACES, Boolean.class);
    }
}
