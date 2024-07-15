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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedType;
import static java.util.Objects.requireNonNull;

public class MongoPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final int MONGO_DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final MongoSession mongoSession;

    @Inject
    public MongoPageSourceProvider(MongoSession mongoSession)
    {
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        MongoTableHandle tableHandle = (MongoTableHandle) table;

        ImmutableList.Builder<MongoColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : requireNonNull(columns, "columns is null")) {
            handles.add((MongoColumnHandle) handle);
        }

        TupleDomain<MongoColumnHandle> dynamicPredicate = dynamicFilter
                .getCurrentPredicate()
                .transformKeys(MongoColumnHandle.class::cast)
                .filter((mongoColumnHandle, domain) -> isPushdownSupportedType(mongoColumnHandle.type()));

        MongoTableHandle newTableHandle;

        if (dynamicFilter == DynamicFilter.EMPTY || tableHandle.limit().isPresent()) {
            newTableHandle = tableHandle;
        }
        else {
            TupleDomain<ColumnHandle> newDomain = tableHandle
                    .constraint()
                    .intersect(dynamicPredicate)
                    .simplify(MONGO_DOMAIN_COMPACTION_THRESHOLD);

            newTableHandle = tableHandle.withConstraint(newDomain);
        }

        if (newTableHandle.constraint().isNone()) {
            return new EmptyPageSource();
        }

        return new MongoPageSource(mongoSession, newTableHandle, handles.build());
    }
}
