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
package io.trino.plugin.thrift.api.datatypes;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.trino.plugin.thrift.api.TrinoThriftBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.trino.plugin.thrift.api.TrinoThriftBlock.timestampData;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.fromLongBasedBlock;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.fromLongBasedColumn;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code timestamps} array are values for each row represented as the number
 * of milliseconds passed since 1970-01-01T00:00:00 UTC.
 * If row is null then value is ignored.
 */
@ThriftStruct
public final class TrinoThriftTimestamp
        implements TrinoThriftColumnData
{
    private final boolean[] nulls;
    private final long[] timestamps;

    @ThriftConstructor
    public TrinoThriftTimestamp(
            @ThriftField(name = "nulls") @Nullable boolean[] nulls,
            @ThriftField(name = "timestamps") @Nullable long[] timestamps)
    {
        checkArgument(sameSizeIfPresent(nulls, timestamps), "nulls and values must be of the same size");
        this.nulls = nulls;
        this.timestamps = timestamps;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public long[] getTimestamps()
    {
        return timestamps;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(TIMESTAMP_MILLIS.equals(desiredType), "type doesn't match: %s", desiredType);
        int numberOfRecords = numberOfRecords();
        long[] timestampsInMicros = new long[numberOfRecords];
        if (timestamps != null) {
            for (int i = 0; i < timestamps.length; i++) {
                timestampsInMicros[i] = multiplyExact(timestamps[i], MICROSECONDS_PER_MILLISECOND);
            }
        }
        return new LongArrayBlock(
                numberOfRecords,
                Optional.ofNullable(nulls),
                timestampsInMicros);
    }

    @Override
    public int numberOfRecords()
    {
        if (nulls != null) {
            return nulls.length;
        }
        if (timestamps != null) {
            return timestamps.length;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TrinoThriftTimestamp other = (TrinoThriftTimestamp) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.timestamps, other.timestamps);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(timestamps));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRecords", numberOfRecords())
                .toString();
    }

    public static TrinoThriftBlock fromBlock(Block block)
    {
        return fromLongBasedBlock(block, TIMESTAMP_MILLIS, (nulls, timestampsInMicros) -> {
            long[] timestampsInMillis = epochMicrosToEpochMillis(timestampsInMicros);
            return timestampData(new TrinoThriftTimestamp(nulls, timestampsInMillis));
        });
    }

    public static TrinoThriftBlock fromRecordSetColumn(RecordSet recordSet, int columnIndex, int totalRecords)
    {
        return fromLongBasedColumn(recordSet, columnIndex, totalRecords, (nulls, timestampsInMicros) -> {
            long[] timestampsInMillis = epochMicrosToEpochMillis(timestampsInMicros);
            return timestampData(new TrinoThriftTimestamp(nulls, timestampsInMillis));
        });
    }

    private static long[] epochMicrosToEpochMillis(@Nullable long[] timestampsInMicros)
    {
        if (timestampsInMicros == null) {
            return null;
        }
        long[] timestampsInMillis = new long[timestampsInMicros.length];
        for (int i = 0; i < timestampsInMicros.length; i++) {
            long epochMicros = timestampsInMicros[i];
            checkArgument(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) == 0, "Not whole milliseconds at position %s: %s", i, epochMicros);
            timestampsInMillis[i] = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
        }
        return timestampsInMillis;
    }

    private static boolean sameSizeIfPresent(boolean[] nulls, long[] timestamps)
    {
        return nulls == null || timestamps == null || nulls.length == timestamps.length;
    }
}
