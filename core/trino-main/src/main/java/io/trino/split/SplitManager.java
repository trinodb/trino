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
package io.trino.split;

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.QueryManagerConfig;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static java.util.Objects.requireNonNull;

public class SplitManager
{
    private final CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider;
    private final int minScheduleSplitBatchSize;

    @Inject
    public SplitManager(CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider, QueryManagerConfig config)
    {
        this.splitManagerProvider = requireNonNull(splitManagerProvider, "splitManagerProvider is null");
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
    }

    public SplitSource getSplits(
            Session session,
            TableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorSplitManager splitManager = splitManagerProvider.getService(catalogHandle);
        if (!isAllowPushdownIntoConnectors(session)) {
            dynamicFilter = DynamicFilter.EMPTY;
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        ConnectorSplitSource source = splitManager.getSplits(
                table.getTransaction(),
                connectorSession,
                table.getConnectorHandle(),
                dynamicFilter,
                constraint);

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogHandle, source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    public SplitSource getSplits(Session session, TableFunctionHandle function)
    {
        CatalogHandle catalogHandle = function.getCatalogHandle();
        ConnectorSplitManager splitManager = splitManagerProvider.getService(catalogHandle);

        ConnectorSplitSource source = splitManager.getSplits(
                function.getTransactionHandle(),
                session.toConnectorSession(catalogHandle),
                function.getSchemaFunctionName(),
                function.getFunctionHandle());

        return new ConnectorAwareSplitSource(catalogHandle, source);
    }
}
