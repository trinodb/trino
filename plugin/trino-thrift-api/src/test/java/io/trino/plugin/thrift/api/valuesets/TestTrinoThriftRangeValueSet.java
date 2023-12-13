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
package io.trino.plugin.thrift.api.valuesets;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.thrift.api.TrinoThriftBlock;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftBigint;
import io.trino.plugin.thrift.api.valuesets.TrinoThriftRangeValueSet.TrinoThriftMarker;
import io.trino.plugin.thrift.api.valuesets.TrinoThriftRangeValueSet.TrinoThriftRange;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.thrift.api.TrinoThriftBlock.bigintData;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftRangeValueSet.TrinoThriftBound.ABOVE;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftRangeValueSet.TrinoThriftBound.BELOW;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftRangeValueSet.TrinoThriftBound.EXACTLY;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftValueSet.fromValueSet;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoThriftRangeValueSet
{
    @Test
    public void testFromValueSetAll()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.all(BIGINT));
        assertThat(thriftValueSet.getRangeValueSet()).isNotNull();
        assertThat(thriftValueSet.getRangeValueSet().getRanges()).isEqualTo(ImmutableList.of(
                new TrinoThriftRange(new TrinoThriftMarker(null, ABOVE), new TrinoThriftMarker(null, BELOW))));
    }

    @Test
    public void testFromValueSetNone()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.none(BIGINT));
        assertThat(thriftValueSet.getRangeValueSet()).isNotNull();
        assertThat(thriftValueSet.getRangeValueSet().getRanges()).isEqualTo(ImmutableList.of());
    }

    @Test
    public void testFromValueSetOf()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.of(BIGINT, 1L, 2L, 3L));
        assertThat(thriftValueSet.getRangeValueSet()).isNotNull();
        assertThat(thriftValueSet.getRangeValueSet().getRanges()).isEqualTo(ImmutableList.of(
                new TrinoThriftRange(new TrinoThriftMarker(longValue(1), EXACTLY), new TrinoThriftMarker(longValue(1), EXACTLY)),
                new TrinoThriftRange(new TrinoThriftMarker(longValue(2), EXACTLY), new TrinoThriftMarker(longValue(2), EXACTLY)),
                new TrinoThriftRange(new TrinoThriftMarker(longValue(3), EXACTLY), new TrinoThriftMarker(longValue(3), EXACTLY))));
    }

    @Test
    public void testFromValueSetOfRangesUnbounded()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 0L)));
        assertThat(thriftValueSet.getRangeValueSet()).isNotNull();
        assertThat(thriftValueSet.getRangeValueSet().getRanges()).isEqualTo(ImmutableList.of(
                new TrinoThriftRange(new TrinoThriftMarker(longValue(0), EXACTLY), new TrinoThriftMarker(null, BELOW))));
    }

    @Test
    public void testFromValueSetOfRangesBounded()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.ofRanges(
                range(BIGINT, -10L, true, -1L, false),
                range(BIGINT, -1L, false, 100L, true)));
        assertThat(thriftValueSet.getRangeValueSet()).isNotNull();
        assertThat(thriftValueSet.getRangeValueSet().getRanges()).isEqualTo(ImmutableList.of(
                new TrinoThriftRange(new TrinoThriftMarker(longValue(-10), EXACTLY), new TrinoThriftMarker(longValue(-1), BELOW)),
                new TrinoThriftRange(new TrinoThriftMarker(longValue(-1), ABOVE), new TrinoThriftMarker(longValue(100), EXACTLY))));
    }

    private static TrinoThriftBlock longValue(long value)
    {
        return bigintData(new TrinoThriftBigint(null, new long[] {value}));
    }
}
