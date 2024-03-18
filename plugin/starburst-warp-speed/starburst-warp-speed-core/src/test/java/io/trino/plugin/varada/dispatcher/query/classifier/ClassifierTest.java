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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.SimplifiedColumns;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class ClassifierTest
{
    protected static final int INT_SIZE = 4;
    protected static final int STR_SIZE = 8;
    protected final Type intType = IntegerType.INTEGER;
    protected final Type varcharType = VarcharType.createVarcharType(10);
    protected PredicateContextFactory predicateContextFactory;

    protected DispatcherTableHandle dispatcherTableHandle;

    protected DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    protected ConnectorSession session;

    void init()
    {
        session = mock(ConnectorSession.class);
        dispatcherTableHandle = mock(DispatcherTableHandle.class);
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Collections.emptySet()));
        predicateContextFactory = new PredicateContextFactory(new GlobalConfiguration(), dispatcherProxiedConnectorTransformer);
    }

    protected ImmutableMap<Integer, ColumnHandle> createCollectColumnsByBlockIndexMap(int numIntCols, int numStrCols)
    {
        return ImmutableMap.copyOf(IntStream.range(0, numIntCols + numStrCols)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), i -> {
                    Type type;
                    if (i < numStrCols) {
                        type = varcharType;
                    }
                    else {
                        type = intType;
                    }
                    return new TestingConnectorColumnHandle(type, "col" + i);
                })));
    }

    protected WarmUpElement createWarmUpElementFromColumnHandle(ColumnHandle columnHandle, WarmUpType warmUpType)
    {
        return createWarmUpElement(new RegularColumn(((TestingConnectorColumnHandle) columnHandle).name()),
                ((TestingConnectorColumnHandle) columnHandle).type(),
                warmUpType);
    }

    protected WarmUpElement createWarmUpElement(VaradaColumn varadaColumn, Type type, WarmUpType warmUpType)
    {
        final boolean isStr = type instanceof VarcharType;
        return WarmUpElement.builder()
                .varadaColumn(varadaColumn)
                .warmUpType(warmUpType)
                .recTypeCode(isStr ? RecTypeCode.REC_TYPE_VARCHAR : RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(isStr ? STR_SIZE : INT_SIZE)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
    }

    protected WarmedWarmupTypes createColumnToWarmUpElementByType(
            Collection<ColumnHandle> columnHandles,
            WarmUpType warmUpType)
    {
        WarmedWarmupTypes.Builder builder = new WarmedWarmupTypes.Builder();

        columnHandles.forEach(x -> builder.add(createWarmUpElementFromColumnHandle(x, warmUpType)));
        return builder.build();
    }

    protected TestingConnectorColumnHandle mockColumnHandle(String columnName, Type type, DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(type, columnName);
        when(dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(eq(columnHandle))).thenReturn(new RegularColumn(columnName));
        when(dispatcherProxiedConnectorTransformer.getColumnType(eq(columnHandle))).thenReturn(type);
        return columnHandle;
    }
}
