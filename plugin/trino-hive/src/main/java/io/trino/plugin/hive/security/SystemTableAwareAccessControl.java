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

package io.trino.plugin.hive.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.security.ForwardingConnectorAccessControl;
import io.trino.plugin.hive.SystemTableProvider;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.util.SystemTables.getSourceTableNameFromSystemTable;
import static io.trino.spi.security.AccessDeniedException.denySelectTable;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static java.util.Objects.requireNonNull;

public class SystemTableAwareAccessControl
        extends ForwardingConnectorAccessControl
{
    private final ConnectorAccessControl delegate;
    private final Set<SystemTableProvider> systemTableProviders;

    @Inject
    public SystemTableAwareAccessControl(ConnectorAccessControl delegate, Set<SystemTableProvider> systemTableProviders)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.systemTableProviders = requireNonNull(systemTableProviders, "systemTableProviders is null");
    }

    @Override
    protected ConnectorAccessControl delegate()
    {
        return delegate;
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        Optional<SchemaTableName> sourceTableName = getSourceTableNameFromSystemTable(systemTableProviders, tableName);
        if (sourceTableName.isPresent()) {
            try {
                checkCanShowColumns(context, sourceTableName.get());
                return;
            }
            catch (AccessDeniedException e) {
                denyShowColumns(tableName.toString());
            }
        }

        delegate.checkCanShowColumns(context, tableName);
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(ConnectorSecurityContext context, Map<SchemaTableName, Set<String>> tableColumns)
    {
        return tableColumns.entrySet().stream()
                .collect(toImmutableMap(
                        Entry::getKey,
                        // TODO call delegate.filterColumns in bulk
                        entry -> filterColumns(context, entry.getKey(), entry.getValue())));
    }

    private Set<String> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columns)
    {
        Optional<SchemaTableName> sourceTableName = getSourceTableNameFromSystemTable(systemTableProviders, tableName);
        if (sourceTableName.isPresent()) {
            // TODO system table may have quite different columns that its source table. It's unclear why it's a good idea to conflate the two.
            return filterColumns(context, sourceTableName.get(), columns);
        }
        return delegate.filterColumns(context, ImmutableMap.of(tableName, columns)).getOrDefault(tableName, ImmutableSet.of());
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        Optional<SchemaTableName> sourceTableName = getSourceTableNameFromSystemTable(systemTableProviders, tableName);
        if (sourceTableName.isPresent()) {
            try {
                checkCanSelectFromColumns(context, sourceTableName.get(), columnNames);
                return;
            }
            catch (AccessDeniedException e) {
                denySelectTable(tableName.toString());
            }
        }

        delegate.checkCanSelectFromColumns(context, tableName, columnNames);
    }
}
