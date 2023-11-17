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
import io.trino.array.BlockBigArray;
import io.trino.array.BooleanBigArray;
import io.trino.array.ByteBigArray;
import io.trino.array.DoubleBigArray;
import io.trino.array.IntBigArray;
import io.trino.array.LongBigArray;
import io.trino.array.ReferenceCountMap;
import io.trino.array.SliceBigArray;
import io.trino.array.SqlMapBigArray;
import io.trino.array.SqlRowBigArray;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapValueBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.util.Reflection;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.util.Map;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
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

        BlockBuilder builder = BIGINT.createBlockBuilder(null, 2);
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

        BlockBuilder builder = BIGINT.createBlockBuilder(null, 1);
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

        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, 1);
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

        BlockBuilder builder = TINYINT.createBlockBuilder(null, 1);
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
        assertThat(deserializedState.getBlock().getLong(0, 0)).isEqualTo(singleState.getBlock().getLong(0, 0));

        SqlMap deserializedMap = deserializedState.getSqlMap();
        SqlMap expectedMap = singleState.getSqlMap();
        assertThat(deserializedMap.getRawKeyBlock().getLong(deserializedMap.getRawOffset(), 0)).isEqualTo(expectedMap.getRawKeyBlock().getLong(expectedMap.getRawOffset(), 0));
        assertThat(deserializedMap.getRawValueBlock().getSlice(deserializedMap.getRawOffset(), 0, 9)).isEqualTo(expectedMap.getRawValueBlock().getSlice(expectedMap.getRawOffset(), 0, 9));

        SqlRow sqlRow = deserializedState.getSqlRow();
        SqlRow expectedSqlRow = singleState.getSqlRow();
        assertThat(VARCHAR.getSlice(sqlRow.getRawFieldBlock(0), sqlRow.getRawIndex())).isEqualTo(VARCHAR.getSlice(expectedSqlRow.getRawFieldBlock(0), expectedSqlRow.getRawIndex()));
        assertThat(BIGINT.getLong(sqlRow.getRawFieldBlock(1), sqlRow.getRawIndex())).isEqualTo(BIGINT.getLong(expectedSqlRow.getRawFieldBlock(1), expectedSqlRow.getRawIndex()));
        assertThat(VARCHAR.getSlice(sqlRow.getRawFieldBlock(2), sqlRow.getRawIndex())).isEqualTo(VARCHAR.getSlice(expectedSqlRow.getRawFieldBlock(2), expectedSqlRow.getRawIndex()));
    }

    private static long getComplexStateRetainedSize(TestComplexState state)
    {
        long retainedSize = instanceSize(state.getClass());
        // reflection is necessary because TestComplexState implementation is generated
        Field[] fields = state.getClass().getDeclaredFields();
        try {
            for (Field field : fields) {
                Class<?> type = field.getType();
                field.setAccessible(true);
                if (type == BlockBigArray.class || type == SqlMapBigArray.class || type == SqlRowBigArray.class || type == BooleanBigArray.class || type == SliceBigArray.class ||
                        type == ByteBigArray.class || type == DoubleBigArray.class || type == LongBigArray.class || type == IntBigArray.class) {
                    MethodHandle sizeOf = Reflection.methodHandle(type, "sizeOf");
                    retainedSize += (long) sizeOf.invokeWithArguments(field.get(state));
                }
            }
        }
        catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return retainedSize;
    }

    private static long getReferenceCountMapOverhead(TestComplexState state)
    {
        long overhead = 0;
        // reflection is necessary because TestComplexState implementation is generated
        Field[] stateFields = state.getClass().getDeclaredFields();
        try {
            for (Field stateField : stateFields) {
                if (stateField.getType() != BlockBigArray.class && stateField.getType() != SqlMapBigArray.class && stateField.getType() != SqlRowBigArray.class && stateField.getType() != SliceBigArray.class) {
                    continue;
                }
                stateField.setAccessible(true);
                Field[] bigArrayFields = stateField.getType().getDeclaredFields();
                for (Field bigArrayField : bigArrayFields) {
                    if (bigArrayField.getType() != ReferenceCountMap.class) {
                        continue;
                    }
                    bigArrayField.setAccessible(true);
                    MethodHandle sizeOf = Reflection.methodHandle(bigArrayField.getType(), "sizeOf");
                    overhead += (long) sizeOf.invokeWithArguments(bigArrayField.get(stateField.get(state)));
                }
            }
        }
        catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return overhead;
    }

    @Test
    public void testComplexStateEstimatedSize()
    {
        Map<String, Type> fieldMap = ImmutableMap.of("Block", new ArrayType(BIGINT), "SqlMap", mapType(BIGINT, VARCHAR));
        AccumulatorStateFactory<TestComplexState> factory = StateCompiler.generateStateFactory(TestComplexState.class, fieldMap);

        TestComplexState groupedState = factory.createGroupedState();
        long initialRetainedSize = getComplexStateRetainedSize(groupedState);
        assertThat(groupedState.getEstimatedSize()).isEqualTo(initialRetainedSize);
        // BlockBigArray or SliceBigArray has an internal map that can grow in size when getting more blocks
        // need to handle the map overhead separately
        initialRetainedSize -= getReferenceCountMapOverhead(groupedState);
        for (int i = 0; i < 1000; i++) {
            long retainedSize = 0;
            ((GroupedAccumulatorState) groupedState).setGroupId(i);
            groupedState.setBoolean(true);
            groupedState.setLong(1);
            groupedState.setDouble(2.0);
            groupedState.setByte((byte) 3);
            groupedState.setInt(4);
            Slice slice = utf8Slice("test");
            retainedSize += slice.getRetainedSize();
            groupedState.setSlice(slice);
            slice = toSlice(1.0, 2.0, 3.0);
            retainedSize += slice.getRetainedSize();
            groupedState.setAnotherSlice(slice);
            groupedState.setYetAnotherSlice(null);
            Block array = createLongsBlock(45);
            retainedSize += array.getRetainedSizeInBytes();
            groupedState.setBlock(array);
            SqlMap sqlMap = MapValueBuilder.buildMapValue(mapType(BIGINT, VARCHAR), 1, (keyBuilder, valueBuilder) -> {
                BIGINT.writeLong(keyBuilder, 123L);
                VARCHAR.writeSlice(valueBuilder, utf8Slice("testBlock"));
            });
            retainedSize += sqlMap.getRetainedSizeInBytes();
            groupedState.setSqlMap(sqlMap);
            SqlRow sqlRow = sqlRowOf(RowType.anonymousRow(VARCHAR, BIGINT, VARCHAR), "a", 777, "b");
            retainedSize += sqlRow.getRetainedSizeInBytes();
            groupedState.setSqlRow(sqlRow);
            assertThat(groupedState.getEstimatedSize()).isEqualTo(initialRetainedSize + retainedSize * (i + 1) + getReferenceCountMapOverhead(groupedState));
        }

        for (int i = 0; i < 1000; i++) {
            long retainedSize = 0;
            ((GroupedAccumulatorState) groupedState).setGroupId(i);
            groupedState.setBoolean(true);
            groupedState.setLong(1);
            groupedState.setDouble(2.0);
            groupedState.setByte((byte) 3);
            groupedState.setInt(4);
            Slice slice = utf8Slice("test");
            retainedSize += slice.getRetainedSize();
            groupedState.setSlice(slice);
            slice = toSlice(1.0, 2.0, 3.0);
            retainedSize += slice.getRetainedSize();
            groupedState.setAnotherSlice(slice);
            groupedState.setYetAnotherSlice(null);
            Block array = createLongsBlock(45);
            retainedSize += array.getRetainedSizeInBytes();
            groupedState.setBlock(array);
            SqlMap sqlMap = MapValueBuilder.buildMapValue(mapType(BIGINT, VARCHAR), 1, (keyBuilder, valueBuilder) -> {
                BIGINT.writeLong(keyBuilder, 123L);
                VARCHAR.writeSlice(valueBuilder, utf8Slice("testBlock"));
            });
            retainedSize += sqlMap.getRetainedSizeInBytes();
            groupedState.setSqlMap(sqlMap);
            SqlRow sqlRow = sqlRowOf(RowType.anonymousRow(VARCHAR, BIGINT, VARCHAR), "a", 777, "b");
            retainedSize += sqlRow.getRetainedSizeInBytes();
            groupedState.setSqlRow(sqlRow);
            assertThat(groupedState.getEstimatedSize()).isEqualTo(initialRetainedSize + retainedSize * 1000 + getReferenceCountMapOverhead(groupedState));
        }
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
