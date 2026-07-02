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
package io.trino.operator.aggregation.state;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.array.BooleanBigArray;
import io.trino.array.DoubleBigArray;
import io.trino.array.LongBigArray;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.function.InOut;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.mapType;
import static io.trino.util.StructuralTestUtil.sqlMapOf;
import static io.trino.util.StructuralTestUtil.sqlRowOf;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStateCompiler
{
    @Test
    public void testPrimitiveNullableLongSerialization()
    {
        AccumulatorStateFactory<NullableLongState> factory = StateCompiler.generateStateFactory(NullableLongState.class);
        AccumulatorStateSerializer<NullableLongState> serializer = StateCompiler.generateStateSerializer(NullableLongState.class);
        NullableLongState state = factory.createSingleState();
        NullableLongState deserializedState = factory.createSingleState();

        state.setValue(2);
        state.setNull(false);

        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(2);
        serializer.serialize(state, builder);
        state.setNull(true);
        serializer.serialize(state, builder);

        Block block = builder.build();

        assertThat(block.isNull(0)).isFalse();
        assertThat(BIGINT.getLong(block, 0)).isEqualTo(state.getValue());
        serializer.deserialize(block, 0, deserializedState);
        assertThat(deserializedState.getValue()).isEqualTo(state.getValue());

        assertThat(block.isNull(1)).isTrue();
    }

    @Test
    public void testPrimitiveLongSerialization()
    {
        AccumulatorStateFactory<LongState> factory = StateCompiler.generateStateFactory(LongState.class);
        AccumulatorStateSerializer<LongState> serializer = StateCompiler.generateStateSerializer(LongState.class);
        LongState state = factory.createSingleState();
        LongState deserializedState = factory.createSingleState();

        state.setValue(2);

        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(1);
        serializer.serialize(state, builder);

        Block block = builder.build();

        assertThat(BIGINT.getLong(block, 0)).isEqualTo(state.getValue());
        serializer.deserialize(block, 0, deserializedState);
        assertThat(deserializedState.getValue()).isEqualTo(state.getValue());
    }

    @Test
    public void testGetSerializedType()
    {
        AccumulatorStateSerializer<LongState> serializer = StateCompiler.generateStateSerializer(LongState.class);
        assertThat(serializer.getSerializedType()).isEqualTo(BIGINT);
    }

    @Test
    public void testPrimitiveBooleanSerialization()
    {
        AccumulatorStateFactory<BooleanState> factory = StateCompiler.generateStateFactory(BooleanState.class);
        AccumulatorStateSerializer<BooleanState> serializer = StateCompiler.generateStateSerializer(BooleanState.class);
        BooleanState state = factory.createSingleState();
        BooleanState deserializedState = factory.createSingleState();

        state.setBoolean(true);

        BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(1);
        serializer.serialize(state, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertThat(deserializedState.isBoolean()).isEqualTo(state.isBoolean());
    }

    @Test
    public void testPrimitiveByteSerialization()
    {
        AccumulatorStateFactory<ByteState> factory = StateCompiler.generateStateFactory(ByteState.class);
        AccumulatorStateSerializer<ByteState> serializer = StateCompiler.generateStateSerializer(ByteState.class);
        ByteState state = factory.createSingleState();
        ByteState deserializedState = factory.createSingleState();

        state.setByte((byte) 3);

        BlockBuilder builder = TINYINT.createFixedSizeBlockBuilder(1);
        serializer.serialize(state, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertThat(deserializedState.getByte()).isEqualTo(state.getByte());
    }

    @Test
    public void testNonPrimitiveSerialization()
    {
        AccumulatorStateFactory<SliceState> factory = StateCompiler.generateStateFactory(SliceState.class);
        AccumulatorStateSerializer<SliceState> serializer = StateCompiler.generateStateSerializer(SliceState.class);
        SliceState state = factory.createSingleState();
        SliceState deserializedState = factory.createSingleState();

        state.setSlice(null);
        BlockBuilder nullBlockBuilder = VARCHAR.createBlockBuilder(null, 1);
        serializer.serialize(state, nullBlockBuilder);
        Block nullBlock = nullBlockBuilder.build();
        serializer.deserialize(nullBlock, 0, deserializedState);
        assertThat(deserializedState.getSlice()).isEqualTo(state.getSlice());

        state.setSlice(utf8Slice("test"));
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertThat(deserializedState.getSlice()).isEqualTo(state.getSlice());
    }

    @Test
    public void testVarianceStateSerialization()
    {
        AccumulatorStateFactory<VarianceState> factory = StateCompiler.generateStateFactory(VarianceState.class);
        AccumulatorStateSerializer<VarianceState> serializer = StateCompiler.generateStateSerializer(VarianceState.class);
        VarianceState singleState = factory.createSingleState();
        VarianceState deserializedState = factory.createSingleState();

        singleState.setMean(1);
        singleState.setCount(2);
        singleState.setM2(3);

        BlockBuilder builder = RowType.anonymous(ImmutableList.of(BIGINT, DOUBLE, DOUBLE)).createBlockBuilder(null, 1);
        serializer.serialize(singleState, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);

        assertThat(deserializedState.getCount()).isEqualTo(singleState.getCount());
        assertThat(deserializedState.getMean()).isEqualTo(singleState.getMean());
        assertThat(deserializedState.getM2()).isEqualTo(singleState.getM2());
    }

    @Test
    public void testComplexSerialization()
    {
        Type arrayType = new ArrayType(BIGINT);
        Type mapType = mapType(BIGINT, VARCHAR);
        Type rowType = RowType.anonymousRow(VARCHAR, BIGINT, VARCHAR);
        Map<String, Type> fieldMap = ImmutableMap.of("Block", arrayType, "SqlMap", mapType, "SqlRow", rowType);
        AccumulatorStateFactory<TestComplexState> factory = StateCompiler.generateStateFactory(TestComplexState.class, fieldMap);
        AccumulatorStateSerializer<TestComplexState> serializer = StateCompiler.generateStateSerializer(TestComplexState.class, fieldMap);
        TestComplexState singleState = factory.createSingleState();
        TestComplexState deserializedState = factory.createSingleState();

        singleState.setBoolean(true);
        singleState.setLong(1);
        singleState.setDouble(2.0);
        singleState.setByte((byte) 3);
        singleState.setInt(4);
        singleState.setSlice(utf8Slice("test"));
        singleState.setAnotherSlice(toSlice(1.0, 2.0, 3.0));
        singleState.setYetAnotherSlice(null);
        Block array = createLongsBlock(45);
        singleState.setBlock(array);
        singleState.setSqlMap(sqlMapOf(BIGINT, VARCHAR, ImmutableMap.of(123L, "testBlock")));
        singleState.setSqlRow(sqlRowOf(RowType.anonymousRow(VARCHAR, BIGINT, VARCHAR), "a", 777, "b"));

        BlockBuilder builder = serializer.getSerializedType().createBlockBuilder(null, 1);
        serializer.serialize(singleState, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);

        assertThat(deserializedState.getBoolean()).isEqualTo(singleState.getBoolean());
        assertThat(deserializedState.getLong()).isEqualTo(singleState.getLong());
        assertThat(deserializedState.getDouble()).isEqualTo(singleState.getDouble());
        assertThat(deserializedState.getByte()).isEqualTo(singleState.getByte());
        assertThat(deserializedState.getInt()).isEqualTo(singleState.getInt());
        assertThat(deserializedState.getSlice()).isEqualTo(singleState.getSlice());
        assertThat(deserializedState.getAnotherSlice()).isEqualTo(singleState.getAnotherSlice());
        assertThat(deserializedState.getYetAnotherSlice()).isEqualTo(singleState.getYetAnotherSlice());
        assertThat(BIGINT.getLong(deserializedState.getBlock(), 0)).isEqualTo(BIGINT.getLong(singleState.getBlock(), 0));

        SqlMap deserializedMap = deserializedState.getSqlMap();
        SqlMap expectedMap = singleState.getSqlMap();
        assertThat(BIGINT.getLong(deserializedMap.getRawKeyBlock(), deserializedMap.getRawOffset())).isEqualTo(BIGINT.getLong(expectedMap.getRawKeyBlock(), expectedMap.getRawOffset()));
        assertThat(VARCHAR.getSlice(deserializedMap.getRawValueBlock(), deserializedMap.getRawOffset())).isEqualTo(VARCHAR.getSlice(expectedMap.getRawValueBlock(), expectedMap.getRawOffset()));

        SqlRow sqlRow = deserializedState.getSqlRow();
        SqlRow expectedSqlRow = singleState.getSqlRow();
        assertThat(VARCHAR.getSlice(sqlRow.getRawFieldBlock(0), sqlRow.getRawIndex())).isEqualTo(VARCHAR.getSlice(expectedSqlRow.getRawFieldBlock(0), expectedSqlRow.getRawIndex()));
        assertThat(BIGINT.getLong(sqlRow.getRawFieldBlock(1), sqlRow.getRawIndex())).isEqualTo(BIGINT.getLong(expectedSqlRow.getRawFieldBlock(1), expectedSqlRow.getRawIndex()));
        assertThat(VARCHAR.getSlice(sqlRow.getRawFieldBlock(2), sqlRow.getRawIndex())).isEqualTo(VARCHAR.getSlice(expectedSqlRow.getRawFieldBlock(2), expectedSqlRow.getRawIndex()));
    }

    @Test
    public void testEstimatedInOutStatesInstanceSizes()
    {
        AccumulatorStateFactory<InOut> factory = StateCompiler.generateInOutStateFactory(BIGINT);
        InOut groupedState = factory.createGroupedState();
        InOut singleState = factory.createSingleState();

        long expectedGroupedSize =
                instanceSize(groupedState.getClass()) +
                        new LongBigArray().sizeOf() + // values, 1024 longs
                        new BooleanBigArray().sizeOf(); // isNull, 1024 booleans

        assertThat(groupedState.getEstimatedSize())
                .isEqualTo(expectedGroupedSize)
                .isEqualTo(17568);

        assertThat(singleState.getEstimatedSize())
                .isEqualTo(instanceSize(singleState.getClass()))
                .isEqualTo(24);
    }

    @Test
    public void testInOutSingleStateAccountsForRetainedObjectValueMemory()
    {
        // VARCHAR (Slice) and a nested row (Block/SqlRow) keep a reference into the source block; the single
        // state must account for the retained size of the stored value, which is not part of the instance size.
        BlockBuilder varcharBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(varcharBuilder, utf8Slice("x".repeat(1024)));
        assertThat(assertInOutSingleStateAccountsForValue(VARCHAR, varcharBuilder.build())).isGreaterThan(1024);

        RowType rowType = RowType.anonymousRow(VARCHAR, BIGINT, VARCHAR);
        BlockBuilder rowBuilder = rowType.createBlockBuilder(null, 1);
        rowType.writeObject(rowBuilder, sqlRowOf(rowType, "x".repeat(1024), 777, "y".repeat(1024)));
        assertThat(assertInOutSingleStateAccountsForValue(rowType, rowBuilder.build())).isGreaterThan(2048);

        // DECIMAL(38) is backed by the self-contained Int128 value object: it retains nothing beyond itself,
        // so the state charges its flat instance size rather than an inflated single-value block size.
        DecimalType decimalType = createDecimalType(38);
        BlockBuilder decimalBuilder = decimalType.createBlockBuilder(null, 1);
        decimalType.writeObject(decimalBuilder, Int128.valueOf(1234567890123456789L, 987654321L));
        assertThat(assertInOutSingleStateAccountsForValue(decimalType, decimalBuilder.build())).isEqualTo(Int128.INSTANCE_SIZE);
    }

    private static long assertInOutSingleStateAccountsForValue(Type type, Block valueBlock)
    {
        AccumulatorStateFactory<InOut> factory = StateCompiler.generateInOutStateFactory(type);
        InOut single = factory.createSingleState();
        long emptySingle = single.getEstimatedSize();
        single.set(valueBlock, 0);
        long perValue = single.getEstimatedSize() - emptySingle;
        assertThat(perValue).isPositive();

        // The stored value round-trips through the output path.
        BlockBuilder out = type.createBlockBuilder(null, 1);
        single.get(out);
        Block outBlock = out.build();
        assertThat(outBlock.isNull(0)).isFalse();
        assertThat(type.getObjectValue(outBlock, 0)).isEqualTo(type.getObjectValue(valueBlock, 0));

        // Setting back to null releases the accounted bytes.
        BlockBuilder nullBuilder = type.createBlockBuilder(null, 1);
        nullBuilder.appendNull();
        single.set(nullBuilder.build(), 0);
        assertThat(single.getEstimatedSize()).isEqualTo(emptySingle);
        return perValue;
    }

    @Test
    public void testInOutStateCompactsSelectedValueInsteadOfRetainingSourcePage()
    {
        // A wide source page: many positions sharing backing arrays. Storing one position must not pin the whole page.
        int positions = 1000;
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, positions);
        for (int i = 0; i < positions; i++) {
            VARCHAR.writeSlice(builder, utf8Slice("x".repeat(1024)));
        }
        Block sourcePage = builder.build();
        long sourceRetained = sourcePage.getRetainedSizeInBytes();
        // The state charges the retained size of the compacted stored value, far smaller than the whole source page.
        long perValue = VARCHAR.getSlice(sourcePage.getSingleValueBlock(7), 0).getRetainedSize();
        assertThat(perValue).isLessThan(sourceRetained / 100);

        AccumulatorStateFactory<InOut> factory = StateCompiler.generateInOutStateFactory(VARCHAR);

        // Single state: accounts for one compacted value, not the source page it was selected from.
        InOut single = factory.createSingleState();
        long emptySingle = single.getEstimatedSize();
        single.set(sourcePage, 7);
        assertThat(single.getEstimatedSize() - emptySingle)
                .isEqualTo(perValue)
                .isLessThan(sourceRetained / 100);

        // Grouped state (specialized BigArray) likewise charges only the compacted value, not the source page.
        InOut groupedInOut = factory.createGroupedState();
        GroupedAccumulatorState grouped = (GroupedAccumulatorState) groupedInOut;
        grouped.ensureCapacity(1);
        long emptyGrouped = groupedInOut.getEstimatedSize();
        grouped.setGroupId(0);
        groupedInOut.set(sourcePage, 7);
        assertThat(groupedInOut.getEstimatedSize() - emptyGrouped).isLessThan(sourceRetained / 100);
    }

    @Test
    public void testEstimatedStateInstanceSizes()
    {
        AccumulatorStateFactory<TestSimpleState> stateFactory = StateCompiler.generateStateFactory(TestSimpleState.class);
        TestSimpleState groupedState = stateFactory.createGroupedState();
        TestSimpleState singleState = stateFactory.createSingleState();

        long expectedGroupedSize = instanceSize(groupedState.getClass()) +
                new LongBigArray().sizeOf() +
                new DoubleBigArray().sizeOf();

        assertThat(groupedState.getEstimatedSize())
                .isEqualTo(expectedGroupedSize)
                .isEqualTo(24744);

        assertThat(singleState.getEstimatedSize())
                .isEqualTo(instanceSize(singleState.getClass()))
                .isEqualTo(32);
    }

    private static Slice toSlice(double... values)
    {
        Slice slice = Slices.allocate(values.length * Double.BYTES);
        SliceOutput output = slice.getOutput();
        for (double value : values) {
            output.writeDouble(value);
        }
        return slice;
    }

    public interface TestSimpleState
            extends AccumulatorState
    {
        long getLong();

        void setLong(long value);

        double getDouble();

        void setDouble(double value);
    }

    public interface TestComplexState
            extends AccumulatorState
    {
        double getDouble();

        void setDouble(double value);

        boolean getBoolean();

        void setBoolean(boolean value);

        long getLong();

        void setLong(long value);

        byte getByte();

        void setByte(byte value);

        int getInt();

        void setInt(int value);

        Slice getSlice();

        void setSlice(Slice slice);

        Slice getAnotherSlice();

        void setAnotherSlice(Slice slice);

        Slice getYetAnotherSlice();

        void setYetAnotherSlice(Slice slice);

        Block getBlock();

        void setBlock(Block block);

        SqlMap getSqlMap();

        void setSqlMap(SqlMap sqlMap);

        SqlRow getSqlRow();

        void setSqlRow(SqlRow sqlRow);
    }

    public interface BooleanState
            extends AccumulatorState
    {
        boolean isBoolean();

        void setBoolean(boolean value);
    }

    public interface ByteState
            extends AccumulatorState
    {
        byte getByte();

        void setByte(byte value);
    }

    public interface SliceState
            extends AccumulatorState
    {
        Slice getSlice();

        void setSlice(Slice slice);
    }
}
