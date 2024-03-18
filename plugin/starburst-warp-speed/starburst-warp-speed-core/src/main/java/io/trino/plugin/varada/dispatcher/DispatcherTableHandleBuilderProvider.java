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
package io.trino.plugin.varada.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherTableHandleBuilderProvider
{
    private final DispatcherProxiedConnectorTransformer transformer;

    @Inject
    public DispatcherTableHandleBuilderProvider(DispatcherProxiedConnectorTransformer transformer)
    {
        this.transformer = requireNonNull(transformer);
    }

    public Builder builder(DispatcherTableHandle dispatcherTableHandle, int predicateThreshold)
    {
        Builder builder = new Builder(transformer, predicateThreshold)
                .proxiedConnectorTableHandle(dispatcherTableHandle.getProxyConnectorTableHandle())
                .schemaName(dispatcherTableHandle.getSchemaName())
                .tableName(dispatcherTableHandle.getTableName())
                .fullPredicate(dispatcherTableHandle.getFullPredicate())
                .warpExpression(dispatcherTableHandle.getWarpExpression())
                .customStats(dispatcherTableHandle.getCustomStats())
                .subsumedPredicates(dispatcherTableHandle.isSubsumedPredicates());
        dispatcherTableHandle.getLimit().ifPresent(builder::limit);
        return builder;
    }

    public Builder builder(int predicateThreshold, ConnectorTableHandle connectorTableHandle)
    {
        SchemaTableName schemaTableName = transformer.getSchemaTableName(connectorTableHandle);
        return new Builder(transformer, predicateThreshold)
                .schemaName(schemaTableName.getSchemaName())
                .tableName(schemaTableName.getTableName())
                .proxiedConnectorTableHandle(connectorTableHandle);
    }

    public static class Builder
    {
        private final DispatcherProxiedConnectorTransformer transformer;
        private String schemaName;
        private String tableName;
        protected final int predicateThreshold;
        protected ConnectorTableHandle proxiedConnectorTableHandle;

        protected OptionalLong limit = OptionalLong.empty();
        protected TupleDomain<ColumnHandle> fullPredicate = TupleDomain.all();
        protected Optional<WarpExpression> warpExpression = Optional.empty();
        protected boolean subsumedPredicates;
        private List<CustomStat> customStats = Collections.emptyList();

        private Builder(DispatcherProxiedConnectorTransformer transformer, int predicateThreshold)
        {
            this.transformer = requireNonNull(transformer);
            this.predicateThreshold = predicateThreshold;
        }

        public Builder schemaName(String schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        public Builder tableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder proxiedConnectorTableHandle(ConnectorTableHandle proxiedConnectorTableHandle)
        {
            this.proxiedConnectorTableHandle = proxiedConnectorTableHandle;
            return this;
        }

        public Builder limit(long limit)
        {
            this.limit = OptionalLong.of(limit);
            return this;
        }

        public Builder fullPredicate(TupleDomain<ColumnHandle> fullPredicate)
        {
            this.fullPredicate = fullPredicate;
            return this;
        }

        public Builder warpExpression(Optional<WarpExpression> warpExpression)
        {
            this.warpExpression = warpExpression;
            return this;
        }

        public Builder customStats(List<CustomStat> customStats)
        {
            this.customStats = customStats;
            return this;
        }

        public Builder subsumedPredicates(boolean subsumedPredicates)
        {
            this.subsumedPredicates = subsumedPredicates;
            return this;
        }

        public DispatcherTableHandle build()
        {
            return new DispatcherTableHandle(schemaName,
                    tableName,
                    limit,
                    fullPredicate,
                    transformer.getSimplifiedColumns(proxiedConnectorTableHandle, fullPredicate, predicateThreshold),
                    proxiedConnectorTableHandle,
                    warpExpression,
                    customStats,
                    subsumedPredicates);
        }
    }
}
