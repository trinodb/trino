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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.mongodb.DBRef;
import com.mongodb.client.MongoCursor;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.mongodb.MongoSession.COLLECTION_NAME;
import static io.trino.plugin.mongodb.MongoSession.DATABASE_NAME;
import static io.trino.plugin.mongodb.MongoSession.ID;
import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.plugin.mongodb.TypeUtils.isJsonType;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToIntBits;
import static java.lang.Math.multiplyExact;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MongoPageSource
        implements ConnectorPageSource
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final int ROWS_PER_REQUEST = 1024;

    private final MongoCursor<Document> cursor;
    private final List<MongoColumnHandle> columns;
    private final List<Type> columnTypes;
    private Document currentDoc;
    private boolean finished;

    private final PageBuilder pageBuilder;

    public MongoPageSource(
            MongoSession mongoSession,
            MongoTableHandle tableHandle,
            List<MongoColumnHandle> columns)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnTypes = columns.stream().map(MongoColumnHandle::getType).collect(toList());
        this.cursor = mongoSession.execute(tableHandle, columns);
        currentDoc = null;

        pageBuilder = new PageBuilder(columnTypes);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0L;
    }

    @Override
    public Page getNextPage()
    {
        verify(pageBuilder.isEmpty());
        for (int i = 0; i < ROWS_PER_REQUEST; i++) {
            if (!cursor.hasNext()) {
                finished = true;
                break;
            }
            currentDoc = cursor.next();

            pageBuilder.declarePosition();
            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                MongoColumnHandle columnHandle = columns.get(column);
                appendTo(columnTypes.get(column), getColumnValue(currentDoc, columnHandle), output);
            }
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void appendTo(Type type, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(SMALLINT)) {
                    type.writeLong(output, Shorts.checkedCast(((Number) value).longValue()));
                }
                else if (type.equals(TINYINT)) {
                    type.writeLong(output, SignedBytes.checkedCast(((Number) value).longValue()));
                }
                else if (type.equals(REAL)) {
                    //noinspection NumericCastThatLosesPrecision
                    type.writeLong(output, floatToIntBits(((float) ((Number) value).doubleValue())));
                }
                else if (type instanceof DecimalType) {
                    Decimal128 decimal = (Decimal128) value;
                    if (decimal.compareTo(Decimal128.NEGATIVE_ZERO) == 0) {
                        type.writeLong(output, encodeShortScaledValue(BigDecimal.ZERO, ((DecimalType) type).getScale()));
                    }
                    else {
                        type.writeLong(output, encodeShortScaledValue(decimal.bigDecimalValue(), ((DecimalType) type).getScale()));
                    }
                }
                else if (type.equals(DATE)) {
                    long utcMillis = ((Date) value).getTime();
                    type.writeLong(output, TimeUnit.MILLISECONDS.toDays(utcMillis));
                }
                else if (type.equals(TIME_MILLIS)) {
                    long millis = UTC_CHRONOLOGY.millisOfDay().get(((Date) value).getTime());
                    type.writeLong(output, multiplyExact(millis, PICOSECONDS_PER_MILLISECOND));
                }
                else if (type.equals(TIMESTAMP_MILLIS)) {
                    // TODO provide correct TIMESTAMP mapping, and respecting session.isLegacyTimestamp()
                    type.writeLong(output, ((Date) value).getTime() * MICROSECONDS_PER_MILLISECOND);
                }
                else if (type.equals(TIMESTAMP_TZ_MILLIS)) {
                    type.writeLong(output, packDateTimeWithZone(((Date) value).getTime(), UTC_KEY));
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (javaType == Int128.class) {
                DecimalType decimalType = (DecimalType) type;
                verify(!decimalType.isShort(), "The type should be long decimal");
                Decimal128 decimal = (Decimal128) value;
                if (decimal.compareTo(Decimal128.NEGATIVE_ZERO) == 0) {
                    type.writeObject(output, Decimals.encodeScaledValue(BigDecimal.ZERO, decimalType.getScale()));
                }
                else {
                    BigDecimal result = decimal.bigDecimalValue();
                    type.writeObject(output, Decimals.encodeScaledValue(result, decimalType.getScale()));
                }
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == Block.class) {
                writeBlock(output, type, value);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
            }
        }
        catch (ClassCastException ignore) {
            // TODO remove (fail clearly), or hide behind a toggle
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private String toVarcharValue(Object value)
    {
        if (value instanceof Collection<?>) {
            return "[" + join(", ", ((Collection<?>) value).stream().map(this::toVarcharValue).collect(toList())) + "]";
        }
        if (value instanceof Document) {
            return ((Document) value).toJson();
        }
        return String.valueOf(value);
    }

    private void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof VarcharType) {
            type.writeSlice(output, utf8Slice(toVarcharValue(value)));
        }
        else if (type instanceof CharType) {
            type.writeSlice(output, truncateToLengthAndTrimSpaces(utf8Slice((String) value), ((CharType) type)));
        }
        else if (type.equals(OBJECT_ID)) {
            type.writeSlice(output, wrappedBuffer(((ObjectId) value).toByteArray()));
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof Binary) {
                type.writeSlice(output, wrappedBuffer(((Binary) value).getData()));
            }
            else {
                output.appendNull();
            }
        }
        else if (type instanceof DecimalType) {
            type.writeObject(output, encodeScaledValue(((Decimal128) value).bigDecimalValue(), ((DecimalType) type).getScale()));
        }
        else if (isJsonType(type)) {
            type.writeSlice(output, jsonParse(utf8Slice(toVarcharValue(value))));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof ArrayType arrayType) {
            if (value instanceof List<?> list) {
                ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> list.forEach(element -> appendTo(arrayType.getElementType(), element, elementBuilder)));
                return;
            }
        }
        else if (type instanceof MapType mapType) {
            if (value instanceof List<?>) {
                ((MapBlockBuilder) output).buildEntry((keyBuilder, valueBuilder) -> {
                    for (Object element : (List<?>) value) {
                        if (!(element instanceof Map<?, ?> document)) {
                            continue;
                        }

                        if (document.containsKey("key") && document.containsKey("value")) {
                            appendTo(mapType.getKeyType(), document.get("key"), keyBuilder);
                            appendTo(mapType.getValueType(), document.get("value"), valueBuilder);
                        }
                    }
                });
                return;
            }
            if (value instanceof Map<?, ?> document) {
                ((MapBlockBuilder) output).buildEntry((keyBuilder, valueBuilder) -> {
                    for (Map.Entry<?, ?> entry : document.entrySet()) {
                        appendTo(mapType.getKeyType(), entry.getKey(), keyBuilder);
                        appendTo(mapType.getValueType(), entry.getValue(), valueBuilder);
                    }
                });
                return;
            }
        }
        else if (type instanceof RowType rowType) {
            List<Field> fields = rowType.getFields();
            if (value instanceof Map<?, ?> mapValue) {
                ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                    for (int i = 0; i < fields.size(); i++) {
                        Field field = fields.get(i);
                        String fieldName = field.getName().orElse("field" + i);
                        appendTo(field.getType(), mapValue.get(fieldName), fieldBuilders.get(i));
                    }
                });
                return;
            }
            if (value instanceof DBRef dbRefValue) {
                checkState(fields.size() == 3, "DBRef should have 3 fields : %s", type);
                ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                    for (int i = 0; i < fields.size(); i++) {
                        Field field = fields.get(i);
                        Type fieldType = field.getType();
                        String fieldName = field.getName().orElseThrow();
                        BlockBuilder builder = fieldBuilders.get(i);
                        switch (fieldName) {
                            case DATABASE_NAME -> appendTo(fieldType, dbRefValue.getDatabaseName(), builder);
                            case COLLECTION_NAME -> appendTo(fieldType, dbRefValue.getCollectionName(), builder);
                            case ID -> appendTo(fieldType, dbRefValue.getId(), builder);
                            default -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected field name for DBRef: " + fieldName);
                        }
                    }
                });
                return;
            }
            if (value instanceof List<?> listValue) {
                ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                    for (int index = 0; index < fields.size(); index++) {
                        if (index < listValue.size()) {
                            appendTo(fields.get(index).getType(), listValue.get(index), fieldBuilders.get(index));
                        }
                        else {
                            fieldBuilders.get(index).appendNull();
                        }
                    }
                });
                return;
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
        }

        // not a convertible value
        output.appendNull();
    }

    private static Object getColumnValue(Document document, MongoColumnHandle mongoColumnHandle)
    {
        Object value = document.get(mongoColumnHandle.getBaseName());
        if (mongoColumnHandle.isBaseColumn()) {
            return value;
        }
        if (value instanceof DBRef dbRefValue) {
            return getDbRefValue(dbRefValue, mongoColumnHandle);
        }
        Document documentValue = (Document) value;
        for (String dereferenceName : mongoColumnHandle.getDereferenceNames()) {
            // When parent field itself is null
            if (documentValue == null) {
                return null;
            }
            value = documentValue.get(dereferenceName);
            if (value instanceof Document nestedDocument) {
                documentValue = nestedDocument;
            }
            else if (value instanceof DBRef dbRefValue) {
                // Assuming DBRefField is the leaf field
                return getDbRefValue(dbRefValue, mongoColumnHandle);
            }
        }
        return value;
    }

    private static Object getDbRefValue(DBRef dbRefValue, MongoColumnHandle columnHandle)
    {
        if (columnHandle.getType() instanceof RowType) {
            return dbRefValue;
        }
        checkArgument(columnHandle.isDbRefField(), "columnHandle is not a dbRef field: " + columnHandle);
        List<String> dereferenceNames = columnHandle.getDereferenceNames();
        checkState(!dereferenceNames.isEmpty(), "dereferenceNames is empty");
        String leafColumnName = dereferenceNames.get(dereferenceNames.size() - 1);
        return switch (leafColumnName) {
            case DATABASE_NAME -> dbRefValue.getDatabaseName();
            case COLLECTION_NAME -> dbRefValue.getCollectionName();
            case ID -> dbRefValue.getId();
            default -> throw new IllegalStateException("Unsupported DBRef column name: " + leafColumnName);
        };
    }

    @Override
    public void close()
    {
        cursor.close();
    }
}
