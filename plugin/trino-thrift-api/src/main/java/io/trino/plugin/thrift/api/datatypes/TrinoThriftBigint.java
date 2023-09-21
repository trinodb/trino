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
import static io.trino.plugin.thrift.api.TrinoThriftBlock.bigintData;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.fromLongBasedBlock;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.fromLongBasedColumn;
import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code longs} array are values for each row. If row is null then value is ignored.
 */
@ThriftStruct
public final class TrinoThriftBigint
        implements TrinoThriftColumnData
{
    private final boolean[] nulls;
    private final long[] longs;

    @ThriftConstructor
    public TrinoThriftBigint(
            @ThriftField(name = "nulls") @Nullable boolean[] nulls,
            @ThriftField(name = "longs") @Nullable long[] longs)
    {
        checkArgument(sameSizeIfPresent(nulls, longs), "nulls and values must be of the same size");
        this.nulls = nulls;
        this.longs = longs;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public long[] getLongs()
    {
        return longs;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(BIGINT.equals(desiredType), "type doesn't match: %s", desiredType);
        int numberOfRecords = numberOfRecords();
        return new LongArrayBlock(
                numberOfRecords,
                Optional.ofNullable(nulls),
                longs == null ? new long[numberOfRecords] : longs);
    }

    @Override
    public int numberOfRecords()
    {
        if (nulls != null) {
            return nulls.length;
        }
        if (longs != null) {
            return longs.length;
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
        TrinoThriftBigint other = (TrinoThriftBigint) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.longs, other.longs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(longs));
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
        return fromLongBasedBlock(block, BIGINT, (nulls, longs) -> bigintData(new TrinoThriftBigint(nulls, longs)));
    }

    public static TrinoThriftBlock fromRecordSetColumn(RecordSet recordSet, int columnIndex, int totalRecords)
    {
        return fromLongBasedColumn(recordSet, columnIndex, totalRecords, (nulls, longs) -> bigintData(new TrinoThriftBigint(nulls, longs)));
    }

    private static boolean sameSizeIfPresent(boolean[] nulls, long[] longs)
    {
        return nulls == null || longs == null || nulls.length == longs.length;
    }
}
