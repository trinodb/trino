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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.varada.log.ShapingLogger;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DispatcherCacheMetadata
        implements ConnectorCacheMetadata
{
    private final ConnectorCacheMetadata proxiedConnectorCacheMetadata;
    private final DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider;
    public final int predicateThreashold;
    private final ObjectMapper objectMapper;
    private final ShapingLogger shapingLogger;

    @Inject
    public DispatcherCacheMetadata(
            @ForWarp ConnectorCacheMetadata proxiedConnectorCacheMetadata,
            DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider,
            GlobalConfiguration globalConfiguration,
            ObjectMapperProvider objectMapperProvider)
    {
        this.proxiedConnectorCacheMetadata = requireNonNull(proxiedConnectorCacheMetadata);
        this.dispatcherTableHandleBuilderProvider = requireNonNull(dispatcherTableHandleBuilderProvider);
        predicateThreashold = requireNonNull(globalConfiguration).getPredicateSimplifyThreshold();
        this.objectMapper = objectMapperProvider.get();
        this.shapingLogger = ShapingLogger.getInstance(
                Logger.get(DispatcherCacheMetadata.class),
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        Optional<CacheTableId> result = proxiedConnectorCacheMetadata.getCacheTableId(dispatcherTableHandle.getProxyConnectorTableHandle());
        if (result.isEmpty()) {
            return result;
        }
        try {
            ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
            if (dispatcherTableHandle.getWarpExpression().isPresent()) {
                values.put("warpExpression ", dispatcherTableHandle.getWarpExpression().get());
            }
            values.put("cacheId", result.get().toString());
            String cacheTableId = objectMapper.writeValueAsString(values.buildOrThrow());
            result = Optional.of(new CacheTableId(cacheTableId));
        }
        catch (JsonProcessingException e) {
            shapingLogger.error(e, "SUBQUERY CACHE: failed to serialize tableHandle=%s", tableHandle);
            result = Optional.empty();
        }
        return result;
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return proxiedConnectorCacheMetadata.getCacheColumnId(((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(), columnHandle);
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = ((DispatcherTableHandle) tableHandle);
        ConnectorTableHandle newProxiedConnectorTableHandle = proxiedConnectorCacheMetadata
                .getCanonicalTableHandle(dispatcherTableHandle.getProxyConnectorTableHandle());
        return dispatcherTableHandleBuilderProvider
                .builder(dispatcherTableHandle, predicateThreashold)
                .warpExpression(Optional.empty())
                .fullPredicate(TupleDomain.all())
                .proxiedConnectorTableHandle(newProxiedConnectorTableHandle)
                .build();
    }
}
