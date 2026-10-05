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
package io.trino.spi.type;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.valueHasNaN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTypeUtils
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    public void testValueHasNaNScalar()
    {
        assertThat(valueHasNaN(DOUBLE, Double.NaN)).isTrue();
        assertThat(valueHasNaN(DOUBLE, 1.0)).isFalse();
        assertThat(valueHasNaN(REAL, (long) floatToRawIntBits(Float.NaN))).isTrue();
        assertThat(valueHasNaN(REAL, (long) floatToRawIntBits(1.0f))).isFalse();
        assertThat(valueHasNaN(BIGINT, 1L)).isFalse();
        assertThat(valueHasNaN(VARCHAR, utf8Slice("NaN"))).isFalse();
    }

    @Test
    public void testValueHasNaNArray()
    {
        ArrayType arrayType = new ArrayType(DOUBLE);
        assertThat(valueHasNaN(arrayType, doubleArray(arrayType, 1.0, 2.0))).isFalse();
        assertThat(valueHasNaN(arrayType, doubleArray(arrayType, 1.0, Double.NaN))).isTrue();
        assertThat(valueHasNaN(arrayType, doubleArray(arrayType))).isFalse();
        assertThat(valueHasNaN(arrayType, buildArrayValue(arrayType, 2, elementBuilder -> {
            elementBuilder.appendNull();
            DOUBLE.writeDouble(elementBuilder, 1.0);
        }))).isFalse();

        // element type that cannot hold NaN
        ArrayType varcharArrayType = new ArrayType(VARCHAR);
        assertThat(valueHasNaN(varcharArrayType, buildArrayValue(varcharArrayType, 1, elementBuilder -> VARCHAR.writeSlice(elementBuilder, utf8Slice("NaN"))))).isFalse();

        // NaN nested two levels deep
        ArrayType nestedArrayType = new ArrayType(arrayType);
        assertThat(valueHasNaN(nestedArrayType, buildArrayValue(nestedArrayType, 1, elementBuilder ->
                arrayType.writeObject(elementBuilder, doubleArray(arrayType, 1.0, Double.NaN))))).isTrue();
        assertThat(valueHasNaN(nestedArrayType, buildArrayValue(nestedArrayType, 1, elementBuilder ->
                arrayType.writeObject(elementBuilder, doubleArray(arrayType, 1.0, 2.0))))).isFalse();
    }

    @Test
    public void testValueHasNaNArrayNotAtStartOfBlock()
    {
        ArrayType arrayType = new ArrayType(DOUBLE);
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 2);
        blockBuilder.buildEntry(elementBuilder -> DOUBLE.writeDouble(elementBuilder, Double.NaN));
        blockBuilder.buildEntry(elementBuilder -> DOUBLE.writeDouble(elementBuilder, 1.0));
        Block block = blockBuilder.build();

        assertThat(valueHasNaN(arrayType, arrayType.getObject(block, 0))).isTrue();
        assertThat(valueHasNaN(arrayType, arrayType.getObject(block, 1))).isFalse();
    }

    @Test
    public void testValueHasNaNMap()
    {
        MapType doubleKeyMapType = new MapType(DOUBLE, BIGINT, TYPE_OPERATORS);
        assertThat(valueHasNaN(doubleKeyMapType, buildMapValue(doubleKeyMapType, 1, (keyBuilder, valueBuilder) -> {
            DOUBLE.writeDouble(keyBuilder, Double.NaN);
            BIGINT.writeLong(valueBuilder, 1);
        }))).isTrue();
        assertThat(valueHasNaN(doubleKeyMapType, buildMapValue(doubleKeyMapType, 1, (keyBuilder, valueBuilder) -> {
            DOUBLE.writeDouble(keyBuilder, 1.0);
            BIGINT.writeLong(valueBuilder, 1);
        }))).isFalse();

        MapType doubleValueMapType = new MapType(BIGINT, DOUBLE, TYPE_OPERATORS);
        assertThat(valueHasNaN(doubleValueMapType, buildMapValue(doubleValueMapType, 2, (keyBuilder, valueBuilder) -> {
            BIGINT.writeLong(keyBuilder, 1);
            DOUBLE.writeDouble(valueBuilder, 1.0);
            BIGINT.writeLong(keyBuilder, 2);
            DOUBLE.writeDouble(valueBuilder, Double.NaN);
        }))).isTrue();
        assertThat(valueHasNaN(doubleValueMapType, buildMapValue(doubleValueMapType, 2, (keyBuilder, valueBuilder) -> {
            BIGINT.writeLong(keyBuilder, 1);
            DOUBLE.writeDouble(valueBuilder, 1.0);
            BIGINT.writeLong(keyBuilder, 2);
            valueBuilder.appendNull();
        }))).isFalse();
    }

    @Test
    public void testValueHasNaNMapNotAtStartOfBlock()
    {
        MapType mapType = new MapType(BIGINT, DOUBLE, TYPE_OPERATORS);
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 2);
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            BIGINT.writeLong(keyBuilder, 1);
            DOUBLE.writeDouble(valueBuilder, Double.NaN);
        });
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            BIGINT.writeLong(keyBuilder, 1);
            DOUBLE.writeDouble(valueBuilder, 1.0);
        });
        Block block = blockBuilder.build();

        assertThat(valueHasNaN(mapType, mapType.getObject(block, 0))).isTrue();
        assertThat(valueHasNaN(mapType, mapType.getObject(block, 1))).isFalse();
    }

    @Test
    public void testValueHasNaNRow()
    {
        RowType rowType = RowType.anonymous(List.of(BIGINT, DOUBLE));
        assertThat(valueHasNaN(rowType, buildRowValue(rowType, fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 1);
            DOUBLE.writeDouble(fieldBuilders.get(1), Double.NaN);
        }))).isTrue();
        assertThat(valueHasNaN(rowType, buildRowValue(rowType, fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 1);
            DOUBLE.writeDouble(fieldBuilders.get(1), 1.0);
        }))).isFalse();
        assertThat(valueHasNaN(rowType, buildRowValue(rowType, fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 1);
            fieldBuilders.get(1).appendNull();
        }))).isFalse();

        // NaN nested in an array field
        ArrayType arrayType = new ArrayType(DOUBLE);
        RowType nestedRowType = RowType.anonymous(List.of(VARCHAR, arrayType));
        assertThat(valueHasNaN(nestedRowType, buildRowValue(nestedRowType, fieldBuilders -> {
            VARCHAR.writeSlice(fieldBuilders.get(0), utf8Slice("a"));
            arrayType.writeObject(fieldBuilders.get(1), doubleArray(arrayType, 1.0, Double.NaN));
        }))).isTrue();
    }

    @Test
    public void testValueHasNaNRowNotAtStartOfBlock()
    {
        RowType rowType = RowType.anonymous(List.of(BIGINT, DOUBLE));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 2);
        blockBuilder.buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 1);
            DOUBLE.writeDouble(fieldBuilders.get(1), Double.NaN);
        });
        blockBuilder.buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 2);
            DOUBLE.writeDouble(fieldBuilders.get(1), 1.0);
        });
        Block block = blockBuilder.build();

        assertThat(valueHasNaN(rowType, rowType.getObject(block, 0))).isTrue();
        assertThat(valueHasNaN(rowType, rowType.getObject(block, 1))).isFalse();
    }

    private static Block doubleArray(ArrayType arrayType, double... values)
    {
        return buildArrayValue(arrayType, values.length, elementBuilder -> {
            for (double value : values) {
                DOUBLE.writeDouble(elementBuilder, value);
            }
        });
    }
}
