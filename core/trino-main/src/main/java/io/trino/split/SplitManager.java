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
import io.trino.connector.CatalogName;
import io.trino.execution.QueryManagerConfig;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static java.util.Objects.requireNonNull;

public class SplitManager
{
    private final ConcurrentMap<CatalogName, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final int minScheduleSplitBatchSize;

    @Inject
    public SplitManager(QueryManagerConfig config)
    {
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
    }

    public void addConnectorSplitManager(CatalogName catalogName, ConnectorSplitManager connectorSplitManager)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorSplitManager, "connectorSplitManager is null");
        checkState(splitManagers.putIfAbsent(catalogName, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", catalogName);
    }

    public void removeConnectorSplitManager(CatalogName catalogName)
    {
        splitManagers.remove(catalogName);
    }

    public SplitSource getSplits(
            Session session,
            TableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorSplitManager splitManager = getConnectorSplitManager(catalogName);
        if (!isAllowPushdownIntoConnectors(session)) {
            dynamicFilter = DynamicFilter.EMPTY;
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        ConnectorSplitSource source = splitManager.getSplits(
                table.getTransaction(),
                connectorSession,
                table.getConnectorHandle(),
                splitSchedulingStrategy,
                dynamicFilter,
                constraint);

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogName, source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    private ConnectorSplitManager getConnectorSplitManager(CatalogName catalogName)
    {
        ConnectorSplitManager result = splitManagers.get(catalogName);
        checkArgument(result != null, "No split manager for connector '%s'", catalogName);
        return result;
    }
}
