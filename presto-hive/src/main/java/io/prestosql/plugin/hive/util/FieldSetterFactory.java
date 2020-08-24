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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
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

import static io.prestosql.plugin.hive.util.HiveUtil.isArrayType;
import static io.prestosql.plugin.hive.util.HiveUtil.isMapType;
import static io.prestosql.plugin.hive.util.HiveUtil.isRowType;
import static io.prestosql.plugin.hive.util.HiveWriteUtils.getHiveDecimal;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
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
        if (type.equals(BooleanType.BOOLEAN)) {
            return new BooleanFieldSetter(rowInspector, row, field);
        }

        if (type.equals(BigintType.BIGINT)) {
            return new BigintFieldBuilder(rowInspector, row, field);
        }

        if (type.equals(IntegerType.INTEGER)) {
            return new IntFieldSetter(rowInspector, row, field);
        }

        if (type.equals(SmallintType.SMALLINT)) {
            return new SmallintFieldSetter(rowInspector, row, field);
        }

        if (type.equals(TinyintType.TINYINT)) {
            return new TinyintFieldSetter(rowInspector, row, field);
        }

        if (type.equals(RealType.REAL)) {
            return new FloatFieldSetter(rowInspector, row, field);
        }

        if (type.equals(DoubleType.DOUBLE)) {
            return new DoubleFieldSetter(rowInspector, row, field);
        }

        if (type instanceof VarcharType) {
            return new VarcharFieldSetter(rowInspector, row, field, type);
        }

        if (type instanceof CharType) {
            return new CharFieldSetter(rowInspector, row, field, type);
        }

        if (type.equals(VarbinaryType.VARBINARY)) {
            return new BinaryFieldSetter(rowInspector, row, field);
        }

        if (type.equals(DateType.DATE)) {
            return new DateFieldSetter(rowInspector, row, field);
        }

        if (type.equals(TIMESTAMP_MILLIS)) {
            return new TimestampFieldSetter(rowInspector, row, field, timeZone);
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new DecimalFieldSetter(rowInspector, row, field, decimalType);
        }

        if (isArrayType(type)) {
            return new ArrayFieldSetter(rowInspector, row, field, type.getTypeParameters().get(0));
        }

        if (isMapType(type)) {
            return new MapFieldSetter(rowInspector, row, field, type.getTypeParameters().get(0), type.getTypeParameters().get(1));
        }

        if (isRowType(type)) {
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
            value.set(BooleanType.BOOLEAN.getBoolean(block, position));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class BigintFieldBuilder
            extends FieldSetter
    {
        private final LongWritable value = new LongWritable();

        public BigintFieldBuilder(SettableStructObjectInspector rowInspector, Object row, StructField field)
        {
            super(rowInspector, row, field);
        }

        @Override
        public void setField(Block block, int position)
        {
            value.set(BigintType.BIGINT.getLong(block, position));
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
            value.set(toIntExact(IntegerType.INTEGER.getLong(block, position)));
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
            value.set(Shorts.checkedCast(SmallintType.SMALLINT.getLong(block, position)));
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
            value.set(SignedBytes.checkedCast(TinyintType.TINYINT.getLong(block, position)));
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
            value.set(DoubleType.DOUBLE.getDouble(block, position));
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
            value.set(intBitsToFloat((int) RealType.REAL.getLong(block, position)));
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
            byte[] bytes = VarbinaryType.VARBINARY.getSlice(block, position).getBytes();
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
            value.set(toIntExact(DateType.DATE.getLong(block, position)));
            rowInspector.setStructFieldData(row, field, value);
        }
    }

    private static class TimestampFieldSetter
            extends FieldSetter
    {
        private final DateTimeZone timeZone;
        private final TimestampWritableV2 value = new TimestampWritableV2();

        public TimestampFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, DateTimeZone timeZone)
        {
            super(rowInspector, row, field);
            this.timeZone = requireNonNull(timeZone, "timeZone is null");
        }

        @Override
        public void setField(Block block, int position)
        {
            long epochMilli = floorDiv(TIMESTAMP_MILLIS.getLong(block, position), MICROSECONDS_PER_MILLISECOND);
            epochMilli = timeZone.convertLocalToUTC(epochMilli, false);
            value.set(Timestamp.ofEpochMilli(epochMilli));
            rowInspector.setStructFieldData(row, field, value);
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

    private static class ArrayFieldSetter
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
                Object element = HiveWriteUtils.getField(elementType, arrayBlock, i);
                list.add(element);
            }

            rowInspector.setStructFieldData(row, field, list);
        }
    }

    private static class MapFieldSetter
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
                Object key = HiveWriteUtils.getField(keyType, mapBlock, i);
                Object value = HiveWriteUtils.getField(valueType, mapBlock, i + 1);
                map.put(key, value);
            }

            rowInspector.setStructFieldData(row, field, map);
        }
    }

    private static class RowFieldSetter
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
                Object element = HiveWriteUtils.getField(fieldTypes.get(i), rowBlock, i);
                value.add(element);
            }

            rowInspector.setStructFieldData(row, field, value);
        }
    }
}
