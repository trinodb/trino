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
package io.trino.plugin.varada.storage.read.predicates;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.PredicateInfo;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.PredicateBufferInfo;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsCachePredicates;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InverseValuesPredicateFillerTest
{
    ByteBuffer low;
    ByteBuffer high;
    private BufferAllocator bufferAllocator;
    private StubsStorageEngineConstants storageEngineConstants;
    private VaradaStatsCachePredicates varadaStatsCachePredicates;

    @BeforeEach
    public void before()
    {
        bufferAllocator = mock(BufferAllocator.class);
        low = ByteBuffer.allocate(100);
        high = ByteBuffer.allocate(100);
        when(bufferAllocator.memorySegment2PredicateBuff(any())).thenReturn(low);
        when(bufferAllocator.createBuffView(any())).thenReturn(high);
        storageEngineConstants = new StubsStorageEngineConstants();
        varadaStatsCachePredicates = VaradaStatsCachePredicates.create(PredicatesCacheService.STATS_CACHE_PREDICATE_KEY);
    }

    @AfterEach
    public void afterEach()
    {
        low = null;
        high = null;
    }

    @Test
    public void testSimple()
    {
        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any())).thenReturn(varadaStatsCachePredicates);
        PredicatesCacheService predicatesCacheService = new PredicatesCacheService(bufferAllocator,
                storageEngineConstants,
                metricsManager);

        Range range1 = Range.range(IntegerType.INTEGER, (long) Integer.MIN_VALUE, false, 8L, false);
        Range range2 = Range.range(IntegerType.INTEGER, 8L, false, (long) Integer.MAX_VALUE, false);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(IntegerType.INTEGER, ImmutableList.of(range1, range2));
        Domain domain = Domain.create(sortedRangeSet, false);
        PredicateInfo predicateInfo = new PredicateInfo(PredicateType.PREDICATE_TYPE_INVERSE_VALUES, FunctionType.FUNCTION_TYPE_NONE, 1, Collections.emptyList(), 8);
        PredicateData predicateData = PredicateData.builder().isCollectNulls(false).predicateInfo(predicateInfo).predicateSize(1).columnType(IntegerType.INTEGER).build();
        PredicateBufferInfo predicateBufferInfo = mock(PredicateBufferInfo.class);
        when(predicateBufferInfo.buff()).thenReturn(MemorySegment.NULL);
        when(bufferAllocator.allocPredicateBuffer(anyInt())).thenReturn(predicateBufferInfo);
        predicatesCacheService.predicateDataToBuffer(predicateData, domain);
        validateResults(high, 0, 8);
    }

    private void validateResults(ByteBuffer buffer, int startPosition, long value)
    {
        int[] actual = new int[2];
        buffer.position(0);
        actual[0] = buffer.getInt(startPosition);
        actual[1] = buffer.getInt(startPosition + Long.BYTES);
        assertThat(Long.valueOf(actual[0])).isEqualTo(value);
    }
}
