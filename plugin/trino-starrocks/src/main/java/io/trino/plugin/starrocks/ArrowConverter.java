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
package io.trino.plugin.starrocks;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.JsonType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ArrowConverter
{
    public static Map<Class<? extends Type>, ArrowFieldConverter> typeConverter = new HashMap<>();

    public static Map<String, Type> typeMap = new HashMap<>();

    private ArrowConverter()
    {
    }

    static {
        typeConverter.put(TinyintType.class, new TinyintConverter());
        typeConverter.put(SmallintType.class, new SmallintConverter());
        typeConverter.put(IntegerType.class, new IntegerConverter());
        typeConverter.put(BigintType.class, new BigintConverter());
        typeConverter.put(RealType.class, new RealConverter());
        typeConverter.put(DoubleType.class, new DoubleConverter());
        // Large int
        typeConverter.put(DecimalType.createDecimalType(38, 0).getClass(), new DynamicDecimalConverter(38, 0));
        // Boolean
        typeConverter.put(BooleanType.class, new BooleanConverter());
        // String
        typeConverter.put(VarcharType.class, new VarcharConverter());
        // TODO:Not Support yet
        // typeConverter.put(VarbinaryType.class, new VarbinaryConverter());
        // Date
        typeConverter.put(DateType.class, new DateConverter());
        typeConverter.put(TimestampType.TIMESTAMP_MILLIS.getClass(), new TimestampConverter());
        // Semi-structured
        typeConverter.put(ArrayType.class, new ArrayConverter());
        typeConverter.put(MapType.class, new MapConverter());
        typeConverter.put(RowType.class, new StructConverter());
        typeConverter.put(JsonType.JSON.getClass(), new VarcharConverter());
    }

    static {
        typeMap.put("boolean", BooleanType.BOOLEAN);
        typeMap.put("tinyint", TinyintType.TINYINT);
        typeMap.put("smallint", SmallintType.SMALLINT);
        typeMap.put("int", IntegerType.INTEGER);
        typeMap.put("integer", IntegerType.INTEGER);
        typeMap.put("bigint", BigintType.BIGINT);
        typeMap.put("bigint unsigned", DecimalType.createDecimalType(38, 0));
        typeMap.put("float", RealType.REAL);
        typeMap.put("double", DoubleType.DOUBLE);
        typeMap.put("date", DateType.DATE);
        typeMap.put("datetime", TimestampType.TIMESTAMP_MILLIS);
        typeMap.put("char", VarcharType.VARCHAR);
        typeMap.put("varchar", VarcharType.VARCHAR);
        typeMap.put("string", VarcharType.VARCHAR);
        typeMap.put("varbinary", VarbinaryType.VARBINARY);
        typeMap.put("largeint", DecimalType.createDecimalType());
    }

    private static class TinyintConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (vector instanceof BitVector) {
                return ((BooleanConverter) typeConverter.get(BooleanType.class)).convert(vector, type, rowCount, dataPosition, blockBuilder);
            }
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                Byte value = ((TinyIntVector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    TinyintType.TINYINT.writeByte(blockBuilder, value);
                }
            }
            return blockBuilder;
        }
    }

    private static class SmallintConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                Short value = ((SmallIntVector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    SmallintType.SMALLINT.writeShort(blockBuilder, value);
                }
            }
            return blockBuilder;
        }
    }

    private static class VarcharConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                Text value = ((VarCharVector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    VarcharType.VARCHAR.writeString(blockBuilder, value.toString());
                }
            }
            return blockBuilder;
        }
    }

    private static class IntegerConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                if (vector instanceof VarCharVector) {
                    VarCharVector valueVectors = (VarCharVector) vector;
                    Text value = valueVectors.getObject(i);
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        Integer intValue = Integer.parseInt(value.toString());
                        IntegerType.INTEGER.writeLong(blockBuilder, intValue);
                    }
                }
                else {
                    Integer value = ((IntVector) vector).getObject(i);
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        IntegerType.INTEGER.writeLong(blockBuilder, value.longValue());
                    }
                }
            }
            return blockBuilder;
        }
    }

    private static class BigintConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                Long value = ((BigIntVector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    BigintType.BIGINT.writeLong(blockBuilder, value);
                }
            }
            return blockBuilder;
        }
    }

    private static class RealConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                Float value = ((Float4Vector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    RealType.REAL.writeFloat(blockBuilder, value);
                }
            }
            return blockBuilder;
        }
    }

    private static class DoubleConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                Double value = ((Float8Vector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    DoubleType.DOUBLE.writeDouble(blockBuilder, value);
                }
            }
            return blockBuilder;
        }
    }

    public static class DynamicDecimalConverter
            implements ArrowFieldConverter
    {
        private final DecimalType decimalType;

        public DynamicDecimalConverter(int precision, int scale)
        {
            this.decimalType = DecimalType.createDecimalType(precision, scale);
        }

        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                if (vector instanceof VarCharVector) {
                    Text value = ((VarCharVector) vector).getObject(i);
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        Int128 int128Value = Int128.valueOf(value.toString());
                        decimalType.writeObject(blockBuilder, int128Value);
                    }
                }
                else if (vector instanceof DecimalVector) {
                    BigDecimal value = ((DecimalVector) vector).getObject(i);
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        int precision = decimalType.getPrecision();
                        if (precision <= 18) {
                            long unscaledValue = value.unscaledValue().longValue();
                            decimalType.writeLong(blockBuilder, unscaledValue);
                        }
                        else {
                            Int128 int128Value = Int128.valueOf(value.unscaledValue());
                            decimalType.writeObject(blockBuilder, int128Value);
                        }
                    }
                }
            }
            return blockBuilder;
        }
    }

    private static class BooleanConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                if (vector.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    boolean value = ((BitVector) vector).get(i) == 1;
                    BooleanType.BOOLEAN.writeBoolean(blockBuilder, value);
                }
            }
            return blockBuilder;
        }
    }

    private static class DateConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                Text value = ((VarCharVector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    LocalDate date = LocalDate.parse(value.toString());
                    DateType.DATE.writeLong(blockBuilder, date.toEpochDay());
                }
            }
            return blockBuilder;
        }
    }

    private static class VarbinaryConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                byte[] value = ((VarBinaryVector) vector).getObject(i);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    VarbinaryType.VARBINARY.writeObject(blockBuilder, value);
                }
            }
            return blockBuilder;
        }
    }

    //timestamp
    private static class TimestampConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            if (blockBuilder == null) {
                blockBuilder = type.createBlockBuilder(null, rowCount);
            }
            for (int i = 0; i < rowCount; i++) {
                if (vector instanceof VarCharVector) {
                    Text value = ((VarCharVector) vector).getObject(i);
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]");
                        LocalDateTime dateTime = LocalDateTime.parse(value.toString(), formatter);
                        long epochMilli = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000;
                        TimestampType.TIMESTAMP_MILLIS.writeLong(blockBuilder, epochMilli);
                    }
                }
                else if (vector instanceof TimeStampMicroVector) {
                    LocalDateTime value = ((TimeStampMicroVector) vector).getObject(i);
                    // epoch seconds to date
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        TimestampType.TIMESTAMP_MILLIS.writeLong(blockBuilder, value.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000000000);
                    }
                }
                else {
                    throw new UnsupportedOperationException("Unsupported type: " + vector.getClass());
                }
            }
            return blockBuilder;
        }
    }

    // Semi-Struct
    // array
    private static class ArrayConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) blockBuilder;
            ListVector listVector = (ListVector) vector;
            ValueVector subVector = listVector.getDataVector();
            String elementType = ((ArrayType) type).getElementType().toString();

            for (int i = 0; i < rowCount; i++) {
                if (listVector.isNull(i)) {
                    arrayBlockBuilder.appendNull();
                    continue;
                }
                int start = listVector.getElementStartIndex(i);
                int end = listVector.getElementEndIndex(i);
                int elementCount = end - start;
                TransferPair transferPair = subVector.getTransferPair(vector.getAllocator());
                transferPair.splitAndTransfer(start, elementCount);
                if (elementType.startsWith("decimal")) {
                    DecimalType innerType = (DecimalType) ((ArrayType) type).getElementType();
                    ArrowFieldConverter converter = new DynamicDecimalConverter(innerType.getPrecision(), innerType.getScale());
                    BlockBuilder tempBlock = DecimalType.createDecimalType(innerType.getPrecision(), innerType.getScale()).createBlockBuilder(null, elementCount);
                    converter.convert((FieldVector) transferPair.getTo(), DecimalType.createDecimalType(innerType.getPrecision(), innerType.getScale()), elementCount, dataPosition, tempBlock);
                    arrayBlockBuilder.buildEntry(elementBuilder -> {
                        ValueBlock elementBlock = tempBlock.buildValueBlock();
                        elementBuilder.appendRange(elementBlock, 0, elementBlock.getPositionCount());
                    });
                }
                else {
                    ArrowFieldConverter converter = typeConverter.get(((ArrayType) type).getElementType().getClass());
                    BlockBuilder tempBlock = ((ArrayType) type).getElementType().createBlockBuilder(null, elementCount);
                    converter.convert((FieldVector) transferPair.getTo(), ((ArrayType) type).getElementType(), elementCount, dataPosition, tempBlock);
                    arrayBlockBuilder.buildEntry(elementBuilder -> {
                        ValueBlock elementBlock = tempBlock.buildValueBlock();
                        elementBuilder.appendRange(elementBlock, 0, elementBlock.getPositionCount());
                    });
                }
                transferPair.getTo().close();
            }
            arrayBlockBuilder.build();
            return arrayBlockBuilder;
        }
    }

    private static class StructConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) blockBuilder;
            StructVector structVector = (StructVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (structVector.isNull(i)) {
                    rowBlockBuilder.appendNull();
                    continue;
                }
                TransferPair transferPair = structVector.getTransferPair(vector.getAllocator());
                transferPair.splitAndTransfer(i, 1);
                StructVector struct = (StructVector) transferPair.getTo();
                List<FieldVector> fieldVectors = struct.getChildrenFromFields();
                rowBlockBuilder.buildEntry(fieldBuilders -> {
                    for (int j = 0; j < fieldVectors.size(); j++) {
                        FieldVector fieldVector = fieldVectors.get(j);
                        Type fieldType = ((RowType) type).getFields().get(j).getType();
                        ArrowFieldConverter converter = typeConverter.get(fieldType.getClass());
                        BlockBuilder fieldBlockBuilder = fieldType.createBlockBuilder(null, 1);
                        converter.convert(fieldVector, fieldType, 1, dataPosition, fieldBlockBuilder);
                        ValueBlock fieldBlock = fieldBlockBuilder.buildValueBlock();
                        fieldBuilders.get(j).appendRange(fieldBlock, 0, fieldBlock.getPositionCount());
                    }
                });
                transferPair.getTo().close();
            }
            return rowBlockBuilder;
        }
    }

    // map
    private static class MapConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) blockBuilder;
            MapVector mapVector = (MapVector) vector;
            MapType rowType = (MapType) type;
            ValueVector subVector = mapVector.getDataVector();
            for (int i = 0; i < rowCount; i++) {
                if (mapVector.isNull(i)) {
                    mapBlockBuilder.appendNull();
                    continue;
                }

                int start = mapVector.getElementStartIndex(i);
                int end = mapVector.getElementEndIndex(i);
                int elementCount = end - start;

                TransferPair transferPair = subVector.getTransferPair(vector.getAllocator());
                transferPair.splitAndTransfer(start, elementCount);
                StructVector structVector = (StructVector) transferPair.getTo();

                ValueVector keysVector = structVector.getChild("key");
                ValueVector valuesVector = structVector.getChild("value");

                String keyType = keysVector.getMinorType().name().toLowerCase(Locale.ROOT);
                String valueType = valuesVector.getMinorType().name().toLowerCase(Locale.ROOT);

                BlockBuilder keysBlockBuilder = rowType.getKeyType().createBlockBuilder(null, elementCount);
                BlockBuilder valuesBlockBuilder = rowType.getValueType().createBlockBuilder(null, elementCount);

                mapBlockBuilder.buildEntry((keyBlockBuilder, valueBlockBuilder) -> {
                    ArrowFieldConverter keyConverter;
                    if (rowType.getKeyType().toString().startsWith("decimal")) {
                        DecimalType innerType = (DecimalType) rowType.getKeyType();
                        keyConverter = new DynamicDecimalConverter(innerType.getPrecision(), innerType.getScale());
                    }
                    else {
                        keyConverter = typeConverter.get(rowType.getKeyType().getClass());
                    }
                    keyConverter.convert((FieldVector) keysVector, rowType.getKeyType(), elementCount, dataPosition, keysBlockBuilder);
                    ArrowFieldConverter valueConverter;

                    if (rowType.getValueType().toString().startsWith("decimal")) {
                        DecimalType innerType = (DecimalType) rowType.getValueType();
                        valueConverter = new DynamicDecimalConverter(innerType.getPrecision(), innerType.getScale());
                    }
                    else {
                        valueConverter = typeConverter.get(rowType.getValueType().getClass());
                    }
                    valueConverter.convert((FieldVector) valuesVector, rowType.getValueType(), elementCount, dataPosition, valuesBlockBuilder);
                    ValueBlock keyBlock = keysBlockBuilder.buildValueBlock();
                    ValueBlock valueBlock = valuesBlockBuilder.buildValueBlock();

                    keyBlockBuilder.appendRange(keyBlock, 0, keyBlock.getPositionCount());
                    valueBlockBuilder.appendRange(valueBlock, 0, valueBlock.getPositionCount());
                });
                transferPair.getTo().close();
            }
            mapBlockBuilder.build();
            return mapBlockBuilder;
        }
    }

    // json
    private static class JsonConverter
            implements ArrowFieldConverter
    {
        @Override
        public BlockBuilder convert(FieldVector vector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
        {
            return null;
        }
    }
}
