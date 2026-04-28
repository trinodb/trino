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
package io.trino.connector.informationschema;

import io.airlift.log.Logger;
import io.trino.FullConnectorSession;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class InformationSchemaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(InformationSchemaPageSourceProvider.class);

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
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = (InformationSchemaTableHandle) tableHandle;
        log.debug(
                "Building information schema table (queryId=%s; tableHandle=%s)",
                session.getQueryId(),
                informationSchemaTableHandle);

        return new InformationSchemaPageSource(
                ((FullConnectorSession) session).getSession(),
                metadata,
                accessControl,
                (InformationSchemaTableHandle) tableHandle,
                columns);
    }
}
