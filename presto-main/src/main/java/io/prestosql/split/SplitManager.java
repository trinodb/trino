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
package io.prestosql.split;

import io.prestosql.Session;
import io.prestosql.connector.ConnectorId;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.Constraint;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SplitManager
{
    private final ConcurrentMap<ConnectorId, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final int minScheduleSplitBatchSize;

    // NOTE: This only used for filling in the table layout if none is present by the time we
    // get splits. DO NOT USE IT FOR ANY OTHER PURPOSE, as it will be removed once table layouts
    // are gone entirely
    private final Metadata metadata;

    @Inject
    public SplitManager(QueryManagerConfig config, Metadata metadata)
    {
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
        this.metadata = metadata;
    }

    public void addConnectorSplitManager(ConnectorId connectorId, ConnectorSplitManager connectorSplitManager)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(connectorSplitManager, "connectorSplitManager is null");
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public void removeConnectorSplitManager(ConnectorId connectorId)
    {
        splitManagers.remove(connectorId);
    }

    public SplitSource getSplits(Session session, TableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        ConnectorId connectorId = table.getConnectorId();
        ConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

        ConnectorSession connectorSession = session.toConnectorSession(connectorId);

        ConnectorTableLayoutHandle layout = table.getLayout()
                .orElseGet(() -> metadata.getLayout(session, table, Constraint.alwaysTrue(), Optional.empty())
                        .get()
                        .getNewTableHandle()
                        .getLayout().get());

        ConnectorSplitSource source = splitManager.getSplits(
                table.getTransaction(),
                connectorSession,
                layout,
                splitSchedulingStrategy);

        SplitSource splitSource = new ConnectorAwareSplitSource(connectorId, table.getTransaction(), source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    private ConnectorSplitManager getConnectorSplitManager(ConnectorId connectorId)
    {
        ConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);
        return result;
    }
}
