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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import org.bson.BsonInvalidOperationException;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.plugin.mongodb.TypeUtils.isJsonType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MongoPageSink
        implements ConnectorPageSink
{
    private final MongoSession mongoSession;
    private final RemoteTableName remoteTableName;
    private final List<MongoColumnHandle> columns;
    private final String implicitPrefix;
    private final Optional<String> pageSinkIdColumnName;
    private final ConnectorPageSinkId pageSinkId;

    public MongoPageSink(
            MongoSession mongoSession,
            RemoteTableName remoteTableName,
            List<MongoColumnHandle> columns,
            String implicitPrefix,
            Optional<String> pageSinkIdColumnName,
            ConnectorPageSinkId pageSinkId)
    {
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.implicitPrefix = requireNonNull(implicitPrefix, "implicitPrefix is null");
        this.pageSinkIdColumnName = requireNonNull(pageSinkIdColumnName, "pageSinkIdColumnName is null");
        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        MongoCollection<Document> collection = mongoSession.getCollection(remoteTableName);
        List<Document> batch = new ArrayList<>(page.getPositionCount());

        for (int position = 0; position < page.getPositionCount(); position++) {
            Document doc = new Document();
            pageSinkIdColumnName.ifPresent(columnName -> doc.append(columnName, pageSinkId.getId()));

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                MongoColumnHandle column = columns.get(channel);
                doc.append(column.baseName(), getObjectValue(columns.get(channel).type(), page.getBlock(channel), position));
            }
            batch.add(doc);
        }

        collection.insertMany(batch, new InsertManyOptions().ordered(true));
        return NOT_BLOCKED;
    }

    private Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            if (type.equals(OBJECT_ID)) {
                return new ObjectId();
            }
            return null;
        }

        if (type.equals(OBJECT_ID)) {
            return new ObjectId(OBJECT_ID.getSlice(block, position).getBytes());
        }
        if (type.equals(BOOLEAN)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        if (type.equals(SMALLINT)) {
            return SMALLINT.getShort(block, position);
        }
        if (type.equals(TINYINT)) {
            return TINYINT.getByte(block, position);
        }
        if (type.equals(REAL)) {
            return REAL.getFloat(block, position);
        }
        if (type.equals(DOUBLE)) {
            return DOUBLE.getDouble(block, position);
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof CharType charType) {
            return padSpaces(charType.getSlice(block, position), charType).toStringUtf8();
        }
        if (type.equals(VARBINARY)) {
            return new Binary(VARBINARY.getSlice(block, position).getBytes());
        }
        if (type.equals(DATE)) {
            int days = DATE.getInt(block, position);
            return LocalDate.ofEpochDay(days);
        }
        if (type.equals(TIME_MILLIS)) {
            long picos = TIME_MILLIS.getLong(block, position);
            return LocalTime.ofNanoOfDay(roundDiv(picos, PICOSECONDS_PER_NANOSECOND));
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            long millisUtc = floorDiv(TIMESTAMP_MILLIS.getLong(block, position), MICROSECONDS_PER_MILLISECOND);
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return LocalDateTime.ofInstant(instant, UTC);
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            long millisUtc = unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(block, position));
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return LocalDateTime.ofInstant(instant, UTC);
        }
        if (type instanceof DecimalType decimalType) {
            return readBigDecimal(decimalType, block, position);
        }
        if (isJsonType(type)) {
            String json = type.getSlice(block, position).toStringUtf8();
            try {
                return Document.parse(json);
            }
            catch (BsonInvalidOperationException e) {
                throw new TrinoException(NOT_SUPPORTED, "Can't convert json to MongoDB Document: " + json, e);
            }
        }
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();

            Block arrayBlock = arrayType.getObject(block, position);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = getObjectValue(elementType, arrayBlock, i);
                list.add(element);
            }

            return unmodifiableList(list);
        }
        if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();

            SqlMap sqlMap = mapType.getObject(block, position);
            int size = sqlMap.getSize();
            int rawOffset = sqlMap.getRawOffset();
            Block rawKeyBlock = sqlMap.getRawKeyBlock();
            Block rawValueBlock = sqlMap.getRawValueBlock();

            // map type is converted into list of fixed keys document
            List<Object> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Map<String, Object> mapValue = new HashMap<>();
                mapValue.put("key", getObjectValue(keyType, rawKeyBlock, rawOffset + i));
                mapValue.put("value", getObjectValue(valueType, rawValueBlock, rawOffset + i));
                values.add(mapValue);
            }

            return unmodifiableList(values);
        }
        if (type instanceof RowType rowType) {
            SqlRow sqlRow = rowType.getObject(block, position);
            int rawIndex = sqlRow.getRawIndex();

            List<Type> fieldTypes = rowType.getTypeParameters();
            if (fieldTypes.size() != sqlRow.getFieldCount()) {
                throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            }

            if (isImplicitRowType(rowType)) {
                List<Object> rowValue = new ArrayList<>();
                for (int i = 0; i < sqlRow.getFieldCount(); i++) {
                    Object element = getObjectValue(fieldTypes.get(i), sqlRow.getRawFieldBlock(i), rawIndex);
                    rowValue.add(element);
                }
                return unmodifiableList(rowValue);
            }

            Map<String, Object> rowValue = new HashMap<>();
            for (int i = 0; i < sqlRow.getFieldCount(); i++) {
                rowValue.put(
                        rowType.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName().orElse("field" + i),
                        getObjectValue(fieldTypes.get(i), sqlRow.getRawFieldBlock(i), rawIndex));
            }
            return unmodifiableMap(rowValue);
        }

        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private boolean isImplicitRowType(Type type)
    {
        return type.getTypeSignature().getParameters()
                .stream()
                .map(TypeSignatureParameter::getNamedTypeSignature)
                .map(NamedTypeSignature::getName)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .allMatch(name -> name.startsWith(implicitPrefix));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @Override
    public void abort() {}
}
