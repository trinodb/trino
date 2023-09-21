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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slices;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public final class SerDeUtils
{
    private SerDeUtils() {}

    public static Block getBlockObject(Type type, Object object, ObjectInspector objectInspector)
    {
        Block serialized = serializeObject(type, null, object, objectInspector);
        return requireNonNull(serialized, "serialized is null");
    }

    public static Block serializeObject(Type type, BlockBuilder builder, Object object, ObjectInspector inspector)
    {
        return serializeObject(type, builder, object, inspector, true);
    }

    // This version supports optionally disabling the filtering of null map key, which should only be used for building test data sets
    // that contain null map keys.  For production, null map keys are not allowed.
    @VisibleForTesting
    public static Block serializeObject(Type type, BlockBuilder builder, Object object, ObjectInspector inspector, boolean filterNullMapKeys)
    {
        switch (inspector.getCategory()) {
            case PRIMITIVE:
                serializePrimitive(type, builder, object, (PrimitiveObjectInspector) inspector);
                return null;
            case LIST:
                return serializeList(type, builder, object, (ListObjectInspector) inspector);
            case MAP:
                return serializeMap((MapType) type, (MapBlockBuilder) builder, object, (MapObjectInspector) inspector, filterNullMapKeys);
            case STRUCT:
                return serializeStruct(type, builder, object, (StructObjectInspector) inspector);
            case UNION:
                return serializeUnion(type, builder, object, (UnionObjectInspector) inspector);
        }
        throw new RuntimeException("Unknown object inspector category: " + inspector.getCategory());
    }

    private static void serializePrimitive(Type type, BlockBuilder builder, Object object, PrimitiveObjectInspector inspector)
    {
        requireNonNull(builder, "builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        switch (inspector.getPrimitiveCategory()) {
            case BOOLEAN:
                type.writeBoolean(builder, ((BooleanObjectInspector) inspector).get(object));
                return;
            case BYTE:
                type.writeLong(builder, ((ByteObjectInspector) inspector).get(object));
                return;
            case SHORT:
                type.writeLong(builder, ((ShortObjectInspector) inspector).get(object));
                return;
            case INT:
                type.writeLong(builder, ((IntObjectInspector) inspector).get(object));
                return;
            case LONG:
                type.writeLong(builder, ((LongObjectInspector) inspector).get(object));
                return;
            case FLOAT:
                type.writeLong(builder, floatToRawIntBits(((FloatObjectInspector) inspector).get(object)));
                return;
            case DOUBLE:
                type.writeDouble(builder, ((DoubleObjectInspector) inspector).get(object));
                return;
            case STRING:
                type.writeSlice(builder, Slices.utf8Slice(((StringObjectInspector) inspector).getPrimitiveJavaObject(object)));
                return;
            case VARCHAR:
                type.writeSlice(builder, Slices.utf8Slice(((HiveVarcharObjectInspector) inspector).getPrimitiveJavaObject(object).getValue()));
                return;
            case CHAR:
                HiveChar hiveChar = ((HiveCharObjectInspector) inspector).getPrimitiveJavaObject(object);
                type.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(hiveChar.getValue()), ((CharType) type).getLength()));
                return;
            case DATE:
                type.writeLong(builder, formatDateAsLong(object, (DateObjectInspector) inspector));
                return;
            case TIMESTAMP:
                TimestampType timestampType = (TimestampType) type;
                DecodedTimestamp timestamp = formatTimestamp(timestampType, object, (TimestampObjectInspector) inspector);
                createTimestampEncoder(timestampType, DateTimeZone.UTC).write(timestamp, builder);
                return;
            case BINARY:
                type.writeSlice(builder, Slices.wrappedBuffer(((BinaryObjectInspector) inspector).getPrimitiveJavaObject(object)));
                return;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                HiveDecimalWritable hiveDecimal = ((HiveDecimalObjectInspector) inspector).getPrimitiveWritableObject(object);
                if (decimalType.isShort()) {
                    type.writeLong(builder, DecimalUtils.getShortDecimalValue(hiveDecimal, decimalType.getScale()));
                }
                else {
                    type.writeObject(builder, DecimalUtils.getLongDecimalValue(hiveDecimal, decimalType.getScale()));
                }
                return;
            case VOID:
            case TIMESTAMPLOCALTZ:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case UNKNOWN:
                // unsupported
        }
        throw new RuntimeException("Unknown primitive type: " + inspector.getPrimitiveCategory());
    }

    private static Block serializeList(Type type, BlockBuilder builder, Object object, ListObjectInspector inspector)
    {
        List<?> list = inspector.getList(object);
        if (list == null) {
            requireNonNull(builder, "builder is null").appendNull();
            return null;
        }

        ArrayType arrayType = (ArrayType) type;
        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        if (builder == null) {
            return buildArrayValue(arrayType, list.size(), valuesBuilder -> buildList(list, arrayType.getElementType(), elementInspector, valuesBuilder));
        }
        ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> buildList(list, arrayType.getElementType(), elementInspector, elementBuilder));
        return null;
    }

    private static void buildList(List<?> list, Type elementType, ObjectInspector elementInspector, BlockBuilder valueBuilder)
    {
        for (Object element : list) {
            serializeObject(elementType, valueBuilder, element, elementInspector);
        }
    }

    private static Block serializeMap(MapType mapType, MapBlockBuilder builder, Object object, MapObjectInspector inspector, boolean filterNullMapKeys)
    {
        Map<?, ?> map = inspector.getMap(object);
        if (map == null) {
            requireNonNull(builder, "builder is null").appendNull();
            return null;
        }

        if (builder == null) {
            return buildMapValue(
                    mapType,
                    map.size(),
                    (keyBuilder, valueBuilder) -> buildMap(mapType, keyBuilder, valueBuilder, map, inspector, filterNullMapKeys));
        }

        builder.buildEntry((keyBuilder, valueBuilder) -> buildMap(mapType, keyBuilder, valueBuilder, map, inspector, filterNullMapKeys));
        return null;
    }

    private static void buildMap(MapType mapType, BlockBuilder keyBuilder, BlockBuilder valueBuilder, Map<?, ?> map, MapObjectInspector inspector, boolean filterNullMapKeys)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueInspector = inspector.getMapValueObjectInspector();

        for (Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (!filterNullMapKeys || entry.getKey() != null) {
                serializeObject(keyType, keyBuilder, entry.getKey(), keyInspector);
                serializeObject(valueType, valueBuilder, entry.getValue(), valueInspector);
            }
        }
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object, StructObjectInspector inspector)
    {
        if (object == null) {
            requireNonNull(builder, "builder is null").appendNull();
            return null;
        }

        RowType rowType = (RowType) type;
        if (builder == null) {
            return buildRowValue(rowType, fieldBuilders -> buildStruct(rowType, object, inspector, fieldBuilders));
        }

        ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> buildStruct(rowType, object, inspector, fieldBuilders));
        return null;
    }

    private static void buildStruct(RowType type, Object object, StructObjectInspector inspector, List<BlockBuilder> fieldBuilders)
    {
        List<Type> typeParameters = type.getTypeParameters();
        List<? extends StructField> allStructFieldRefs = inspector.getAllStructFieldRefs();
        checkArgument(typeParameters.size() == allStructFieldRefs.size());
        for (int i = 0; i < typeParameters.size(); i++) {
            StructField field = allStructFieldRefs.get(i);
            serializeObject(typeParameters.get(i), fieldBuilders.get(i), inspector.getStructFieldData(object, field), field.getFieldObjectInspector());
        }
    }

    // Use row blocks to represent union objects when reading
    private static Block serializeUnion(Type type, BlockBuilder builder, Object object, UnionObjectInspector inspector)
    {
        if (object == null) {
            requireNonNull(builder, "builder is null").appendNull();
            return null;
        }

        RowType rowType = (RowType) type;
        if (builder == null) {
            return buildRowValue(rowType, fieldBuilders -> buildUnion(rowType, object, inspector, fieldBuilders));
        }
        ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> buildUnion(rowType, object, inspector, fieldBuilders));
        return null;
    }

    private static void buildUnion(RowType rowType, Object object, UnionObjectInspector inspector, List<BlockBuilder> fieldBuilders)
    {
        byte tag = inspector.getTag(object);
        TINYINT.writeLong(fieldBuilders.get(0), tag);

        List<Type> typeParameters = rowType.getTypeParameters();
        for (int i = 1; i < typeParameters.size(); i++) {
            if (i == tag + 1) {
                serializeObject(typeParameters.get(i), fieldBuilders.get(i), inspector.getField(object), inspector.getObjectInspectors().get(tag));
            }
            else {
                fieldBuilders.get(i).appendNull();
            }
        }
    }

    @SuppressWarnings("deprecation")
    private static long formatDateAsLong(Object object, DateObjectInspector inspector)
    {
        if (object instanceof LazyDate) {
            return ((LazyDate) object).getWritableObject().getDays();
        }
        if (object instanceof DateWritable) {
            return ((DateWritable) object).getDays();
        }
        return inspector.getPrimitiveJavaObject(object).toEpochDay();
    }

    private static DecodedTimestamp formatTimestamp(TimestampType type, Object object, TimestampObjectInspector inspector)
    {
        long epochSecond;
        int nanoOfSecond;

        if (object instanceof TimestampWritable timestamp) {
            epochSecond = timestamp.getSeconds();
            nanoOfSecond = timestamp.getNanos();
        }
        else {
            Timestamp timestamp = inspector.getPrimitiveJavaObject(object);
            epochSecond = timestamp.toEpochSecond();
            nanoOfSecond = timestamp.getNanos();
        }

        nanoOfSecond = (int) round(nanoOfSecond, 9 - type.getPrecision());
        if (nanoOfSecond == NANOSECONDS_PER_SECOND) { // round nanos up to seconds
            epochSecond += 1;
            nanoOfSecond = 0;
        }

        return new DecodedTimestamp(epochSecond, nanoOfSecond);
    }
}
