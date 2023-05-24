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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.hive.util.HiveWriteUtils.getField;
import static io.trino.plugin.hive.util.HiveWriteUtils.getHiveDecimal;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class FieldSetterFactory
{
    private final DateTimeZone timeZone;

    public FieldSetterFactory(DateTimeZone timeZone)
    {
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    public FieldSetter create(SettableStructObjectInspector rowInspector, Object row, StructField field, Type type)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanFieldSetter(rowInspector, row, field);
        }
        if (BIGINT.equals(type)) {
            return new BigintFieldSetter(rowInspector, row, field);
        }
        if (INTEGER.equals(type)) {
            return new IntFieldSetter(rowInspector, row, field);
        }
        if (SMALLINT.equals(type)) {
            return new SmallintFieldSetter(rowInspector, row, field);
        }
        if (TINYINT.equals(type)) {
            return new TinyintFieldSetter(rowInspector, row, field);
        }
        if (REAL.equals(type)) {
            return new FloatFieldSetter(rowInspector, row, field);
        }
        if (DOUBLE.equals(type)) {
            return new DoubleFieldSetter(rowInspector, row, field);
        }
        if (type instanceof VarcharType) {
            return new VarcharFieldSetter(rowInspector, row, field, type);
        }
        if (type instanceof CharType) {
            return new CharFieldSetter(rowInspector, row, field, type);
        }
        if (VARBINARY.equals(type)) {
            return new BinaryFieldSetter(rowInspector, row, field);
        }
        if (DATE.equals(type)) {
            return new DateFieldSetter(rowInspector, row, field);
        }
        if (type instanceof TimestampType timestampType) {
            return new TimestampFieldSetter(rowInspector, row, field, timestampType, timeZone);
        }
        if (type instanceof DecimalType decimalType) {
            return new DecimalFieldSetter(rowInspector, row, field, decimalType);
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayFieldSetter(rowInspector, row, field, arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return new MapFieldSetter(rowInspector, row, field, mapType.getKeyType(), mapType.getValueType());
        }
        if (type instanceof RowType) {
            return new RowFieldSetter(rowInspector, row, field, type.getTypeParameters());
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public abstract static class FieldSetter
    {
        protected final SettableStructObjectInspector rowInspector;
        protected final Object row;
        protected final StructField field;

        private FieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            this.rowInspector = requireNonNull(rowInspector, "rowInspector is null");
            this.row = requireNonNull(row, "row is null");
            this.field = requireNonNull(field, "field is null");
        }

        public abstract void setField(Block block, int position);
    }

    private static class BooleanFieldSetter
            extends FieldSetter
    {
        private final BooleanWritable value = new BooleanWritable();

        public BooleanFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(BOOLEAN.getBoolean(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class BigintFieldSetter
            extends FieldSetter
    {
        private final LongWritable value = new LongWritable();

        public BigintFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(BIGINT.getLong(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class IntFieldSetter
            extends FieldSetter
    {
        private final IntWritable value = new IntWritable();

        public IntFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(INTEGER.getInt(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class SmallintFieldSetter
            extends FieldSetter
    {
        private final ShortWritable value = new ShortWritable();

        public SmallintFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(SMALLINT.getShort(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class TinyintFieldSetter
            extends FieldSetter
    {
        private final ByteWritable value = new ByteWritable();

        public TinyintFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(TINYINT.getByte(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class DoubleFieldSetter
            extends FieldSetter
    {
        private final DoubleWritable value = new DoubleWritable();

        public DoubleFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(DOUBLE.getDouble(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class FloatFieldSetter
            extends FieldSetter
    {
        private final FloatWritable value = new FloatWritable();

        public FloatFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(REAL.getFloat(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class VarcharFieldSetter
            extends FieldSetter
    {
        private final Text value = new Text();
        private final Type type;

        public VarcharFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, Type type)
        {
            super(rowInspector, row, field);
            this.type = type;
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(type.getSlice(block, position).getBytes());
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class CharFieldSetter
            extends FieldSetter
    {
        private final Text value = new Text();
        private final Type type;

        public CharFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, Type type)
        {
            super(rowInspector, row, field);
            this.type = type;
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(type.getSlice(block, position).getBytes());
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class BinaryFieldSetter
            extends FieldSetter
    {
        private final BytesWritable value = new BytesWritable();

        public BinaryFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            byte[] bytes = VARBINARY.getSlice(block, position).getBytes();
            value.set(bytes, 0, bytes.length);
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class DateFieldSetter
            extends FieldSetter
    {
        private final DateWritableV2 value = new DateWritableV2();

        public DateFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(DATE.getInt(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class TimestampFieldSetter
            extends FieldSetter
    {
        private final DateTimeZone timeZone;
        private final TimestampType type;
        private final TimestampWritableV2 value = new TimestampWritableV2();

        public TimestampFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, TimestampType type, DateTimeZone timeZone)
        {
            super(rowInspector, row, field);
            this.type = requireNonNull(type, "type is null");
            this.timeZone = requireNonNull(timeZone, "timeZone is null");
        }

        @Override
        public void setField(Block block, int position)
        {
            long epochMicros;
            int picosOfMicro;
            if (type.isShort()) {
                epochMicros = type.getLong(block, position);
                picosOfMicro = 0;
            }
            else {
                LongTimestamp longTimestamp = (LongTimestamp) type.getObject(block, position);
                epochMicros = longTimestamp.getEpochMicros();
                picosOfMicro = longTimestamp.getPicosOfMicro();
            }

            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            long picosOfSecond = (long) floorMod(epochMicros, MICROSECONDS_PER_SECOND) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;

            epochSeconds = convertLocalEpochSecondsToUtc(epochSeconds);
            // no rounding since the data has nanosecond precision, at most
            int nanosOfSecond = toIntExact(picosOfSecond / PICOSECONDS_PER_NANOSECOND);

            Timestamp timestamp = Timestamp.ofEpochSecond(epochSeconds, nanosOfSecond);
            value.set(timestamp);
            rowInspector.setStructFieldData(row, field, value);
        }

        private long convertLocalEpochSecondsToUtc(long epochSeconds)
        {
            long epochMillis = epochSeconds * MILLISECONDS_PER_SECOND;
            epochMillis = timeZone.convertLocalToUTC(epochMillis, false);
            return epochMillis / MILLISECONDS_PER_SECOND;
        }
    }

    private static class DecimalFieldSetter
            extends FieldSetter
    {
        private final HiveDecimalWritable value = new HiveDecimalWritable();
        private final DecimalType decimalType;

        public DecimalFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, DecimalType decimalType)
        {
            super(rowInspector, row, field);
            this.decimalType = decimalType;
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(getHiveDecimal(decimalType, block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private class ArrayFieldSetter
            extends FieldSetter
    {
        private final Type elementType;

        public ArrayFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, Type elementType)
        {
            super(rowInspector, row, field);
            this.elementType = requireNonNull(elementType, "elementType is null");
        }

        @Override
        public void setField(Block block, int position)
        {
            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(getField(timeZone, elementType, arrayBlock, i));
            }

            rowInspector.setStructFieldData(row, field, list);
        }
    }

    private class MapFieldSetter
            extends FieldSetter
    {
        private final Type keyType;
        private final Type valueType;

        public MapFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, Type keyType, Type valueType)
        {
            super(rowInspector, row, field);
            this.keyType = requireNonNull(keyType, "keyType is null");
            this.valueType = requireNonNull(valueType, "valueType is null");
        }

        @Override
        public void setField(Block block, int position)
        {
            Block mapBlock = block.getObject(position, Block.class);
            Map<Object, Object> map = new HashMap<>(mapBlock.getPositionCount() * 2);
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(
                        getField(timeZone, keyType, mapBlock, i),
                        getField(timeZone, valueType, mapBlock, i + 1));
            }

            rowInspector.setStructFieldData(row, field, map);
        }
    }

    private class RowFieldSetter
            extends FieldSetter
    {
        private final List<Type> fieldTypes;

        public RowFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, List<Type> fieldTypes)
        {
            super(rowInspector, row, field);
            this.fieldTypes = ImmutableList.copyOf(fieldTypes);
        }

        @Override
        public void setField(Block block, int position)
        {
            Block rowBlock = block.getObject(position, Block.class);

            // TODO reuse row object and use FieldSetters, like we do at the top level
            // Ideally, we'd use the same recursive structure starting from the top, but
            // this requires modeling row types in the same way we model table rows
            // (multiple blocks vs all fields packed in a single block)
            List<Object> value = new ArrayList<>(fieldTypes.size());
            for (int i = 0; i < fieldTypes.size(); i++) {
                value.add(getField(timeZone, fieldTypes.get(i), rowBlock, i));
            }

            rowInspector.setStructFieldData(row, field, value);
        }
    }
}
