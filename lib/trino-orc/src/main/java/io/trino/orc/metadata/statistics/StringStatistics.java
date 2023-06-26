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
package io.trino.orc.metadata.statistics;

import io.airlift.slice.Slice;
import io.trino.orc.metadata.statistics.StatisticsHasher.Hashable;
import jakarta.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.io.BaseEncoding.base16;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.String.format;

public class StringStatistics
        implements RangeStatistics<Slice>, Hashable
{
    // 1 byte to denote if null + 4 bytes to denote offset
    public static final long STRING_VALUE_BYTES_OVERHEAD = Byte.BYTES + Integer.BYTES;

    private static final int INSTANCE_SIZE = instanceSize(StringStatistics.class);

    @Nullable
    private final Slice minimum;
    @Nullable
    private final Slice maximum;
    private final long sum;

    public StringStatistics(@Nullable Slice minimum, @Nullable Slice maximum, long sum)
    {
        if (minimum != null && maximum != null && minimum.compareTo(maximum) > 0) {
            throw new IllegalArgumentException(format(
                    "minimum is not less than or equal to maximum: '%s' [%s], '%s' [%s]",
                    minimum.toStringUtf8(),
                    base16().encode(minimum.getBytes()),
                    maximum.toStringUtf8(),
                    base16().encode(maximum.getBytes())));
        }
        this.minimum = minimum;
        this.maximum = maximum;
        this.sum = sum;
    }

    @Override
    public Slice getMin()
    {
        return minimum;
    }

    @Override
    public Slice getMax()
    {
        return maximum;
    }

    public long getSum()
    {
        return sum;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + (minimum == null ? 0 : minimum.getRetainedSize()) + ((maximum == null || maximum == minimum) ? 0 : maximum.getRetainedSize());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StringStatistics that = (StringStatistics) o;
        return Objects.equals(minimum, that.minimum) &&
                Objects.equals(maximum, that.maximum) &&
                Objects.equals(sum, that.sum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minimum, maximum, sum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", minimum == null ? "<null>" : minimum.toStringUtf8())
                .add("max", maximum == null ? "<null>" : maximum.toStringUtf8())
                .add("sum", sum)
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putOptionalSlice(minimum)
                .putOptionalSlice(maximum)
                .putLong(sum);
    }
}
