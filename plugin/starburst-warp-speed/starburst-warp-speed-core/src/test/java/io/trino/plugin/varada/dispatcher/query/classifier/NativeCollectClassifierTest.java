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
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.QueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeCollectClassifierTest
        extends ClassifierTest
{
    protected static final int COLLECT_NULLS_SIZE = 64 * 1024;
    protected static final int COLLECT_REC_SIZE_PER_BYTE = 64 * 1024;

    protected BufferAllocator bufferAllocator;
    protected QueryContext baseQueryContext;

    @BeforeEach
    public void before()
    {
        init();
        this.bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(INT_SIZE))).thenReturn(INT_SIZE * COLLECT_REC_SIZE_PER_BYTE);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(STR_SIZE))).thenReturn(STR_SIZE * COLLECT_REC_SIZE_PER_BYTE);
        when(bufferAllocator.getMatchCollectRecordBufferSize(eq(INT_SIZE))).thenReturn(INT_SIZE * COLLECT_REC_SIZE_PER_BYTE);
        when(bufferAllocator.getQueryNullBufferSize(any())).thenReturn(COLLECT_NULLS_SIZE);
        this.baseQueryContext = new QueryContext(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE), ImmutableMap.of());
    }

    @Test
    public void testCollectedColumnsLimitation()
    {
        final int numVaradaCols = 3;
        final int numProxyCols = 5;

        QueryContext classify = classifyNativeCollect(numVaradaCols, 0, numVaradaCols + numProxyCols, 0, false);

        assertThat(classify.getNativeQueryCollectDataList().size()).isEqualTo(numVaradaCols);
        assertThat(classify.getRemainingCollectColumnByBlockIndex().size()).isEqualTo(numProxyCols);
        assertFalse(classify.getNativeQueryCollectDataList().stream()
                .map(QueryCollectData::getBlockIndex)
                .anyMatch(classify.getRemainingCollectColumnByBlockIndex().keySet()::contains));
    }

    @Test
    public void testCollectedColumnsOrderFixedSizeAndString()
    {
        final int numIntCols = 3;
        final int numVaradaStrCols = 2;
        final int numProxyStrCols = 5;
        final int numStrCols = numVaradaStrCols + numProxyStrCols;
        final int numVaradaCols = numIntCols + numVaradaStrCols;

        QueryContext classify = classifyNativeCollect(numIntCols, numVaradaStrCols, numIntCols, numStrCols, false);

        assertThat(classify.getNativeQueryCollectDataList().size()).isEqualTo(numVaradaCols);
        assertThat(classify.getRemainingCollectColumnByBlockIndex().size()).isEqualTo(numProxyStrCols);
        assertFalse(classify.getNativeQueryCollectDataList().stream()
                .map(QueryCollectData::getBlockIndex)
                .anyMatch(classify.getRemainingCollectColumnByBlockIndex().keySet()::contains));

        int i;
        for (i = 0; i < numIntCols; i++) {
            assertThat(classify.getNativeQueryCollectDataList().get(i).getWarmUpElement().getRecTypeLength()).isEqualTo(INT_SIZE);
        }
        for (; i < numVaradaCols; i++) {
            assertThat(classify.getNativeQueryCollectDataList().get(i).getWarmUpElement().getRecTypeLength()).isEqualTo(STR_SIZE);
        }
    }

    @Test
    public void testCollectedColumnsOrderWithMatchCollect()
    {
        final int numVaradaIntCols = 1;
        final int numIntCols = 3;
        final int numStrCols = 2;

        QueryContext classify = classifyNativeCollect(numVaradaIntCols, 0, numIntCols, numStrCols, true);

        assertThat(classify.getNativeQueryCollectDataList().size()).isEqualTo(numVaradaIntCols);
        assertThat(classify.getRemainingCollectColumnByBlockIndex().size()).isEqualTo(numIntCols + numStrCols - numVaradaIntCols);
        assertFalse(classify.getNativeQueryCollectDataList().stream()
                .map(QueryCollectData::getBlockIndex)
                .anyMatch(classify.getRemainingCollectColumnByBlockIndex().keySet()::contains));

        for (int i = 0; i < numVaradaIntCols; i++) {
            assertThat(classify.getNativeQueryCollectDataList().get(i).getWarmUpElement().getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_BASIC);
            assertThat(classify.getNativeQueryCollectDataList().get(i).getWarmUpElement().getRecTypeLength()).isEqualTo(INT_SIZE);
        }
    }

    private NativeCollectClassifier createNativeCollectClassifier(int numIntCols, int numStrCols)
    {
        return new NativeCollectClassifier(INT_SIZE * COLLECT_REC_SIZE_PER_BYTE, // one column
                1,
                (INT_SIZE * COLLECT_REC_SIZE_PER_BYTE + COLLECT_NULLS_SIZE) * numIntCols +
                        (STR_SIZE * COLLECT_REC_SIZE_PER_BYTE + COLLECT_NULLS_SIZE) * numStrCols +
                        10000,
                100 * 1024 * 1024,
                16 * 1024,
                128,
                bufferAllocator,
                dispatcherProxiedConnectorTransformer);
    }

    private WarmedWarmupTypes createColumnToWarmUpElementByTypeWithMatchCollect(
            Collection<ColumnHandle> columnHandles)
    {
        ColumnHandle basicColumnHandle = columnHandles.stream().filter(h -> !(((TestingConnectorColumnHandle) h).type() instanceof VarcharType)).findFirst().orElseThrow();

        ImmutableMap<WarmUpType, ImmutableMap<VaradaColumn, WarmUpElement>> res = ImmutableMap.of(WarmUpType.WARM_UP_TYPE_DATA,
                ImmutableMap.copyOf(columnHandles
                        .stream()
                        .filter(h -> !((TestingConnectorColumnHandle) h).name().equals(((TestingConnectorColumnHandle) basicColumnHandle).name()))
                        .collect(Collectors.toMap((columnHandle) -> (VaradaColumn) new RegularColumn(((TestingConnectorColumnHandle) columnHandle).name()), h -> createWarmUpElementFromColumnHandle(h, WarmUpType.WARM_UP_TYPE_DATA)))),
                WarmUpType.WARM_UP_TYPE_BASIC,
                ImmutableMap.of(new RegularColumn(((TestingConnectorColumnHandle) basicColumnHandle).name()),
                        createWarmUpElementFromColumnHandle(basicColumnHandle, WarmUpType.WARM_UP_TYPE_BASIC)));
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        for (Map.Entry<WarmUpType, ImmutableMap<VaradaColumn, WarmUpElement>> v : res.entrySet()) {
            for (Map.Entry<VaradaColumn, WarmUpElement> column : v.getValue().entrySet()) {
                warmedWarmupTypes.add(column.getValue());
            }
        }
        return warmedWarmupTypes.build();
    }

    private QueryContext createQueryContext(ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex,
            boolean withMatchCollect)
    {
        if (withMatchCollect) {
            ColumnHandle basicColumnHandle = collectColumnsByBlockIndex.values().stream().filter(h -> !(((TestingConnectorColumnHandle) h).type() instanceof VarcharType)).findFirst().orElseThrow();
            return baseQueryContext.asBuilder()
                    .matchData(Optional.of(BasicBloomQueryMatchData.builder()
                            .varadaColumn(new RegularColumn(((TestingConnectorColumnHandle) basicColumnHandle).name()))
                            .warmUpElement(createWarmUpElementFromColumnHandle(basicColumnHandle, WarmUpType.WARM_UP_TYPE_BASIC))
                            .nativeExpression(NativeExpression.builder()
                                    .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                                    .functionType(FunctionType.FUNCTION_TYPE_NONE)
                                    .domain(Domain.all(((TestingConnectorColumnHandle) basicColumnHandle).type()))
                                    .build())
                            .build()))
                    .remainingCollectColumnByBlockIndex(collectColumnsByBlockIndex)
                    .build();
        }
        return baseQueryContext.asBuilder()
                .remainingCollectColumnByBlockIndex(collectColumnsByBlockIndex)
                .build();
    }

    private QueryContext classifyNativeCollect(int numVaradaIntCols, int numVaradaStrCols, int numIntCols, int numStrCols, boolean withMatchCollect)
    {
        NativeCollectClassifier nativeCollectClassifier = createNativeCollectClassifier(numVaradaIntCols, numVaradaStrCols);
        ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex = createCollectColumnsByBlockIndexMap(numIntCols, numStrCols);
        for (ColumnHandle ch : collectColumnsByBlockIndex.values()) {
            RegularColumn varadaColumn = new RegularColumn(((TestingConnectorColumnHandle) ch).name());
            Type warmUpType = ((TestingConnectorColumnHandle) ch).type();
            when(dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(eq(ch))).thenReturn(varadaColumn);
            when(dispatcherProxiedConnectorTransformer.getColumnType(eq(ch))).thenReturn(warmUpType);
        }
        WarmedWarmupTypes warmedWarmupTypes;
        if (withMatchCollect) {
            warmedWarmupTypes = createColumnToWarmUpElementByTypeWithMatchCollect(collectColumnsByBlockIndex.values());
        }
        else {
            warmedWarmupTypes = createColumnToWarmUpElementByType(collectColumnsByBlockIndex.values(), WarmUpType.WARM_UP_TYPE_DATA);
        }
        ClassifyArgs classifyArgs = new ClassifyArgs(mock(DispatcherTableHandle.class),
                mock(RowGroupData.class),
                mock(PredicateContextData.class),
                collectColumnsByBlockIndex,
                warmedWarmupTypes,
                false,
                true,
                false);
        QueryContext queryContext = createQueryContext(collectColumnsByBlockIndex, withMatchCollect);

        return nativeCollectClassifier.classify(classifyArgs, queryContext);
    }
}
