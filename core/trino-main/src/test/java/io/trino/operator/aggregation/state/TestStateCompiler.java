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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
