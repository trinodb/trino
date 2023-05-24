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
package io.trino.plugin.blackhole;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class BlackHolePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ListeningScheduledExecutorService executorService;

    public BlackHolePageSourceProvider(ListeningScheduledExecutorService executorService)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        BlackHoleTableHandle table = (BlackHoleTableHandle) tableHandle;

        ImmutableList.Builder<Type> builder = ImmutableList.builder();

        for (ColumnHandle column : columns) {
            builder.add(((BlackHoleColumnHandle) column).getColumnType());
        }
        List<Type> types = builder.build();

        Page page = generateZeroPage(types, table.getRowsPerPage(), table.getFieldsLength());
        return new BlackHolePageSource(page, table.getPagesPerSplit(), executorService, table.getPageProcessingDelay());
    }

    private Page generateZeroPage(List<Type> types, int rowsCount, int fieldLength)
    {
        byte[] constantBytes = new byte[fieldLength];
        Arrays.fill(constantBytes, (byte) 42);
        Slice constantSlice = Slices.wrappedBuffer(constantBytes);

        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = createZeroBlock(types.get(i), rowsCount, constantSlice);
        }

        return new Page(rowsCount, blocks);
    }

    private Block createZeroBlock(Type type, int rowsCount, Slice constantSlice)
    {
        checkArgument(isSupportedType(type), "Unsupported type [%s]", type);

        Slice slice;
        // do not exceed varchar limit
        if (type instanceof VarcharType && !((VarcharType) type).isUnbounded()) {
            slice = constantSlice.slice(0, Math.min(((VarcharType) type).getBoundedLength(), constantSlice.length()));
        }
        else {
            slice = constantSlice;
        }

        BlockBuilder builder;
        if (type instanceof FixedWidthType) {
            builder = type.createBlockBuilder(null, rowsCount);
        }
        else {
            builder = type.createBlockBuilder(null, rowsCount, slice.length());
        }

        for (int i = 0; i < rowsCount; i++) {
            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
                type.writeBoolean(builder, false);
            }
            else if (javaType == long.class) {
                type.writeLong(builder, 0);
            }
            else if (javaType == double.class) {
                type.writeDouble(builder, 0.0);
            }
            else if (javaType == Slice.class) {
                requireNonNull(slice, "slice is null");
                type.writeSlice(builder, slice, 0, slice.length());
            }
            else if (type instanceof DecimalType decimalType && !decimalType.isShort()) {
                type.writeObject(builder, Int128.ZERO);
            }
            else {
                throw new UnsupportedOperationException("Unknown javaType: " + javaType.getName());
            }
        }
        return builder.build();
    }

    private static boolean isSupportedType(Type type)
    {
        return isNumericType(type) ||
                type instanceof BooleanType ||
                type instanceof DateType ||
                type instanceof TimestampType ||
                type instanceof VarcharType ||
                type instanceof VarbinaryType;
    }

    public static boolean isNumericType(Type type)
    {
        return type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType;
    }
}
