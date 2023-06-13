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

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.mongodb.DBRef;
import com.mongodb.client.MongoCursor;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.mongodb.MongoSession.COLLECTION_NAME;
import static io.trino.plugin.mongodb.MongoSession.DATABASE_NAME;
import static io.trino.plugin.mongodb.MongoSession.ID;
import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.plugin.mongodb.TypeUtils.isArrayType;
import static io.trino.plugin.mongodb.TypeUtils.isJsonType;
import static io.trino.plugin.mongodb.TypeUtils.isMapType;
import static io.trino.plugin.mongodb.TypeUtils.isRowType;
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
import static java.util.stream.Collectors.toList;

public class MongoPageSource
        implements ConnectorPageSource
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final int ROWS_PER_REQUEST = 1024;

    private final MongoCursor<Document> cursor;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private Document currentDoc;
    private boolean finished;

    private final PageBuilder pageBuilder;

    public MongoPageSource(
            MongoSession mongoSession,
            MongoTableHandle tableHandle,
            List<MongoColumnHandle> columns)
    {
        this.columnNames = columns.stream().map(MongoColumnHandle::getName).collect(toList());
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
                appendTo(columnTypes.get(column), currentDoc.get(columnNames.get(column)), output);
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
                    type.writeLong(output, encodeShortScaledValue(((Decimal128) value).bigDecimalValue(), ((DecimalType) type).getScale()));
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
                BigDecimal decimal = ((Decimal128) value).bigDecimalValue();
                type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
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
        if (isArrayType(type)) {
            if (value instanceof List<?>) {
                BlockBuilder builder = output.beginBlockEntry();

                ((List<?>) value).forEach(element ->
                        appendTo(type.getTypeParameters().get(0), element, builder));

                output.closeEntry();
                return;
            }
        }
        else if (isMapType(type)) {
            if (value instanceof List<?>) {
                BlockBuilder builder = output.beginBlockEntry();
                for (Object element : (List<?>) value) {
                    if (!(element instanceof Map<?, ?> document)) {
                        continue;
                    }

                    if (document.containsKey("key") && document.containsKey("value")) {
                        appendTo(type.getTypeParameters().get(0), document.get("key"), builder);
                        appendTo(type.getTypeParameters().get(1), document.get("value"), builder);
                    }
                }

                output.closeEntry();
                return;
            }
            if (value instanceof Map<?, ?> document) {
                BlockBuilder builder = output.beginBlockEntry();
                for (Map.Entry<?, ?> entry : document.entrySet()) {
                    appendTo(type.getTypeParameters().get(0), entry.getKey(), builder);
                    appendTo(type.getTypeParameters().get(1), entry.getValue(), builder);
                }
                output.closeEntry();
                return;
            }
        }
        else if (isRowType(type)) {
            if (value instanceof Map<?, ?> mapValue) {
                BlockBuilder builder = output.beginBlockEntry();

                for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
                    TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
                    String fieldName = parameter.getNamedTypeSignature().getName().orElse("field" + i);
                    appendTo(type.getTypeParameters().get(i), mapValue.get(fieldName), builder);
                }
                output.closeEntry();
                return;
            }
            if (value instanceof DBRef dbRefValue) {
                BlockBuilder builder = output.beginBlockEntry();

                checkState(type.getTypeParameters().size() == 3, "DBRef should have 3 fields : %s", type);
                for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
                    TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
                    Type fieldType = type.getTypeParameters().get(i);
                    String fieldName = parameter.getNamedTypeSignature().getName().orElseThrow();
                    switch (fieldName) {
                        case DATABASE_NAME -> appendTo(fieldType, dbRefValue.getDatabaseName(), builder);
                        case COLLECTION_NAME -> appendTo(fieldType, dbRefValue.getCollectionName(), builder);
                        case ID -> appendTo(fieldType, dbRefValue.getId(), builder);
                        default -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected field name for DBRef: " + fieldName);
                    }
                }

                output.closeEntry();
                return;
            }
            if (value instanceof List<?> listValue) {
                BlockBuilder builder = output.beginBlockEntry();
                for (int index = 0; index < type.getTypeParameters().size(); index++) {
                    if (index < listValue.size()) {
                        appendTo(type.getTypeParameters().get(index), listValue.get(index), builder);
                    }
                    else {
                        builder.appendNull();
                    }
                }
                output.closeEntry();
                return;
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
        }

        // not a convertible value
        output.appendNull();
    }

    @Override
    public void close()
    {
        cursor.close();
    }
}
