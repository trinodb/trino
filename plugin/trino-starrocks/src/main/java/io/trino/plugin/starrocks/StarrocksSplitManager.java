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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;

import java.util.concurrent.CompletableFuture;

import static io.trino.spi.connector.DynamicFilter.NOT_BLOCKED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StarrocksSplitManager
        implements ConnectorSplitManager
{
    private final StarrocksClient client;

    @Inject
    public StarrocksSplitManager(StarrocksClient client)
    {
        this.client = client;
    }

    private static CompletableFuture<?> whenCompleted(DynamicFilter dynamicFilter)
    {
        if (dynamicFilter.isAwaitable()) {
            return dynamicFilter.isBlocked().thenCompose(ignored -> whenCompleted(dynamicFilter));
        }
        return NOT_BLOCKED;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        long timeoutMillis = StarrocksSessionProperties.getDynamicFilteringWaitTimeout(session).toMillis();
        int tupleDomainLimit = StarrocksSessionProperties.getTupleDomainLimit(session);
        StarrocksTableHandle starrocksTableHandle = (StarrocksTableHandle) table;
        if (timeoutMillis == 0 || !dynamicFilter.isAwaitable()) {
            return getSplitSource(
                    table,
                    starrocksTableHandle.getConstraint().intersect(dynamicFilter.getCurrentPredicate()),
                    tupleDomainLimit);
        }
        CompletableFuture<?> dynamicFilterFuture = whenCompleted(dynamicFilter)
                .completeOnTimeout(null, timeoutMillis, MILLISECONDS);
        CompletableFuture<ConnectorSplitSource> splitSourceFuture = dynamicFilterFuture.thenApply(
                ignored -> getSplitSource(table, starrocksTableHandle.getConstraint().intersect(dynamicFilter.getCurrentPredicate()), tupleDomainLimit));
        return new StarrocksSplitSource(client, starrocksTableHandle, dynamicFilterFuture, splitSourceFuture, session);
    }

    private ConnectorSplitSource getSplitSource(
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> dynamicFilter,
            int tupleDomainLimit)
    {
        StarrocksTableHandle handle = (StarrocksTableHandle) table;

        return new FixedSplitSource(client.getFeClient().buildStarrocksSplits(handle, dynamicFilter, tupleDomainLimit));
    }
}
