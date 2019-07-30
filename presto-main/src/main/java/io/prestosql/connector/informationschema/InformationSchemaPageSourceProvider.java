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
package io.prestosql.connector.informationschema;

import io.prestosql.FullConnectorSession;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.connector.informationschema.InformationSchemaMetadata.enumerateSchemas;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.isTablesEnumeratingTable;
import static java.util.Objects.requireNonNull;

public class InformationSchemaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    public InformationSchemaPageSourceProvider(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns)
    {
        InformationSchemaTableHandle handle = (InformationSchemaTableHandle) tableHandle;
        Optional<Set<String>> schemaNames = handle.getSchemas();
        if (isTablesEnumeratingTable(handle.getSchemaTableName()) && !schemaNames.isPresent()) {
            schemaNames = Optional.of(enumerateSchemas(metadata, session, handle.getCatalogName()));
            handle = new InformationSchemaTableHandle(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), schemaNames, handle.getTables());
        }
        return new InformationSchemaPageSource(
                ((FullConnectorSession) session).getSession(),
                metadata,
                accessControl,
                handle,
                columns);
    }
}
