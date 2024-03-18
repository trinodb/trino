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
package io.trino.plugin.varada.juffer;

import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.PredicateInfo;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsCachePredicates;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PredicatesCacheServiceTest
{
    StubsStorageEngineConstants storageEngineConstants;
    VaradaStatsCachePredicates varadaStatsCachePredicates;
    BufferAllocator bufferAllocator;
    PredicatesCacheService predicatesCacheService;

    static Stream<Arguments> poolTypes()
    {
        return Stream.of(
                arguments(PredicateBufferPoolType.SMALL),
                arguments(PredicateBufferPoolType.LARGE));
    }

    @BeforeEach
    public void before()
    {
        storageEngineConstants = new StubsStorageEngineConstants();
        varadaStatsCachePredicates = VaradaStatsCachePredicates.create(PredicatesCacheService.STATS_CACHE_PREDICATE_KEY);
        bufferAllocator = mock(BufferAllocator.class);
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        when(bufferAllocator.memorySegment2PredicateBuff(any())).thenReturn(byteBuffer);
        when(bufferAllocator.createBuffView(byteBuffer)).thenReturn(byteBuffer);
        when(bufferAllocator.getBufferSize(isA(PredicateBufferPoolType.class))).thenReturn(5);
        when(bufferAllocator.getPoolSize(isA(PredicateBufferPoolType.class))).thenReturn(2);
        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any())).thenReturn(varadaStatsCachePredicates);
        predicatesCacheService = new PredicatesCacheService(bufferAllocator,
                storageEngineConstants,
                metricsManager);
    }

    @ParameterizedTest
    @MethodSource("poolTypes")
    public void testBufferPoolTypes(PredicateBufferPoolType predicateBufferPoolType)
    {
        PredicateData predicateData = buildMockedPredicateData(MemorySegment.NULL, predicateBufferPoolType, 1);
        ArgumentCaptor<PredicateCacheData> argument = ArgumentCaptor.forClass(PredicateCacheData.class);
        Domain domain = Domain.singleValue(IntegerType.INTEGER, (long) 1);
        PredicateCacheData actualBufferHandle = predicatesCacheService.getOrCreatePredicateBufferId(predicateData, domain).orElseThrow();
        assertThat(actualBufferHandle.isUsed()).isTrue();
        predicatesCacheService.markFinished(List.of(actualBufferHandle));
        verify(bufferAllocator, never()).freePredicateBuffer(argument.capture());
        assertThat(actualBufferHandle.isUsed()).isFalse();
    }

    private PredicateData buildMockedPredicateData(MemorySegment buff, PredicateBufferPoolType predicateBufferPoolType, int size)
    {
        PredicateData predicateData = mock(PredicateData.class);
        PredicateBufferInfo predicateBufferInfo = new PredicateBufferInfo(buff, predicateBufferPoolType);
        when(bufferAllocator.getRequiredPredicateBufferType(eq(size))).thenReturn(predicateBufferPoolType);
        when(bufferAllocator.allocPredicateBuffer(eq(size))).thenReturn(predicateBufferInfo);
        when(predicateData.getPredicateSize()).thenReturn(size);
        when(predicateData.getPredicateInfo()).thenReturn(new PredicateInfo(PredicateType.PREDICATE_TYPE_VALUES, FunctionType.FUNCTION_TYPE_NONE, size, Collections.emptyList(), 8));
        return predicateData;
    }
}
