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
package io.trino.json;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.json.JsonItemBuilder.JsonItemWriter;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;

import static io.airlift.slice.Slices.utf8Slice;

/// Encodes tree-form JSON values as typed-item bytes.
public final class JsonItems
{
    private JsonItems() {}

    /// Walks a tree-form [Json] and emits its typed-item byte encoding via
    /// [JsonItemBuilder]. Called by tree-backed
    /// [Json] impls' `encoding()` to lazily materialize the byte form when a
    /// tree value crosses a Block / wire boundary. The tree is walked twice:
    /// once to compute the exact byte size (so the encoder allocates a single
    /// right-sized buffer, no growth-and-copy reallocs), once to emit.
    public static Json encodeTree(Json tree)
    {
        int innerSize = treeInnerSize(tree);
        return JsonItemBuilder.encode(writer -> writeTreeJson(writer, tree), Byte.BYTES + innerSize);
    }

    /// Encodes a tree-form value directly into `output`, with no intermediate slice — used by the
    /// block builder to write a value into the column's buffer.
    public static void encodeTreeInto(DynamicSliceOutput output, Json tree)
    {
        JsonItemBuilder.encodeInto(output, writer -> writeTreeJson(writer, tree));
    }

    /// Encodes a single [TypedValue] scalar as a stand-alone JSON item.
    public static Json encodeScalar(TypedValue scalar)
    {
        return JsonItemBuilder.encode(writer -> writeTypedValue(writer, scalar), Byte.BYTES + scalarInnerSize(scalar));
    }

    /// Inner-item byte size (no VERSION prefix) for any [Json] — used as the
    /// exact buffer-size hint for [#encodeTree]. For byte-backed forms the
    /// size is the existing view length; for tree forms it walks recursively.
    public static int treeInnerSize(Json node)
    {
        if (node instanceof EncodedJson encoded) {
            return encoded.viewEnd() - encoded.viewOffset();
        }
        return switch (node.kind()) {
            case NULL, ERROR -> Byte.BYTES;
            case SCALAR -> scalarInnerSize((TypedValue) node);
            case ARRAY -> arrayInnerSize(node);
            case OBJECT -> objectInnerSize(node);
        };
    }

    private static int arrayInnerSize(Json array)
    {
        if (array instanceof JsonArray treeArray && treeArray.elements().size() >= JsonItemEncoding.INDEXED_CONTAINER_THRESHOLD) {
            return arrayIndexedInnerSize(treeArray);
        }
        int size = JsonItemEncoding.CONTAINER_HEADER_SIZE;
        int[] total = {size};
        array.forEachArrayElement(element -> total[0] += treeInnerSize(element));
        return total[0];
    }

    private static int arrayIndexedInnerSize(JsonArray array)
    {
        int count = array.elements().size();
        // tag(1) + count(4) + (count+1)*4 offsets + sum(item inner sizes)
        int size = Byte.BYTES + Integer.BYTES + (count + 1) * Integer.BYTES;
        for (Json element : array.elements()) {
            size += treeInnerSize(element);
        }
        return size;
    }

    private static int objectInnerSize(Json object)
    {
        if (object instanceof JsonObject treeObject && shouldEmitIndexedObject(treeObject)) {
            return objectIndexedInnerSize(treeObject);
        }
        int size = JsonItemEncoding.CONTAINER_HEADER_SIZE;
        if (object instanceof JsonObject treeObject) {
            for (JsonObjectMember member : treeObject.members()) {
                size += Integer.BYTES + member.key().length();
                size += treeInnerSize(member.value());
            }
            return size;
        }
        int[] total = {size};
        object.forEachObjectMember((key, value) -> {
            total[0] += Integer.BYTES + utf8Slice(key).length();
            total[0] += treeInnerSize(value);
        });
        return total[0];
    }

    private static boolean shouldEmitIndexedObject(JsonObject object)
    {
        int count = object.members().size();
        // OBJECT_INDEXED's binary-search resolver assumes unique keys (sort permutation
        // identifies one entry per key); tree-form objects allowed duplicates per
        // SQL:2023 §9.42, so fall back to plain when duplicates are present.
        return count >= JsonItemEncoding.INDEXED_CONTAINER_THRESHOLD
                && count <= JsonItemEncoding.MAX_OBJECT_INDEXED_COUNT
                && !object.hasDuplicateKeys();
    }

    private static int objectIndexedInnerSize(JsonObject object)
    {
        int count = object.members().size();
        // tag(1) + count(4) + count*2 permutation + (count+1)*4 offsets + sum(entry sizes)
        int size = Byte.BYTES + Integer.BYTES + count * Short.BYTES + (count + 1) * Integer.BYTES;
        for (JsonObjectMember member : object.members()) {
            size += Integer.BYTES + member.key().length();
            size += treeInnerSize(member.value());
        }
        return size;
    }

    private static int scalarInnerSize(TypedValue scalar)
    {
        // Mirrors JsonItemEncoding.appendXxx layouts. Tag(1) + TypeTag(1) + value bytes.
        Type type = scalar.type();
        int header = Byte.BYTES + Byte.BYTES;
        if (type == BooleanType.BOOLEAN || type == TinyintType.TINYINT) {
            return header + Byte.BYTES;
        }
        if (type == SmallintType.SMALLINT) {
            return header + Short.BYTES;
        }
        if (type == IntegerType.INTEGER || type == RealType.REAL) {
            return header + Integer.BYTES;
        }
        if (type == BigintType.BIGINT || type == DoubleType.DOUBLE) {
            return header + Long.BYTES;
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            Slice value = (Slice) scalar.value();
            return header + Integer.BYTES + value.length();
        }
        if (type instanceof DecimalType decimalType) {
            return header + Integer.BYTES + Integer.BYTES + Byte.BYTES + (decimalType.isShort() ? Long.BYTES : 2 * Long.BYTES);
        }
        if (type == NumberType.NUMBER) {
            TrinoNumber number = (TrinoNumber) scalar.value();
            return header + numberInnerSize(number);
        }
        throw new IllegalArgumentException("Unsupported scalar type for tree encoding: " + type);
    }

    private static int numberInnerSize(TrinoNumber value)
    {
        // Mirrors JsonItemEncoding.appendNumber body layout.
        return switch (value.toBigDecimal()) {
            case TrinoNumber.NotANumber _, TrinoNumber.Infinity _ -> Byte.BYTES;
            case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> Byte.BYTES + Integer.BYTES + Integer.BYTES + decimal.unscaledValue().toByteArray().length;
        };
    }

    private static void writeTreeJson(JsonItemWriter writer, Json node)
    {
        switch (node) {
            case JsonNullValue _ -> writer.nullValue();
            case JsonErrorValue _ -> writer.errorValue();
            case TypedValue scalar -> writeTypedValue(writer, scalar);
            case EncodedJson encoded -> writer.nest(encoded);
            case JsonArray array -> {
                if (array.elements().size() >= JsonItemEncoding.INDEXED_CONTAINER_THRESHOLD) {
                    writer.startIndexedArray();
                    for (Json element : array.elements()) {
                        writeTreeJson(writer, element);
                    }
                    writer.endIndexedArray();
                }
                else {
                    writer.startArray();
                    for (Json element : array.elements()) {
                        writeTreeJson(writer, element);
                    }
                    writer.endArray();
                }
            }
            case JsonObject object -> {
                if (shouldEmitIndexedObject(object)) {
                    writer.startIndexedObject();
                    for (JsonObjectMember member : object.members()) {
                        writer.fieldName(member.key().toStringUtf8());
                        writeTreeJson(writer, member.value());
                    }
                    writer.endIndexedObject();
                }
                else {
                    writer.startObject();
                    for (JsonObjectMember member : object.members()) {
                        writer.fieldName(member.key().toStringUtf8());
                        writeTreeJson(writer, member.value());
                    }
                    writer.endObject();
                }
            }
        }
    }

    private static void writeTypedValue(JsonItemWriter writer, TypedValue value)
    {
        Type type = value.type();
        switch (type) {
            case BooleanType _ -> writer.booleanValue((Boolean) value.value());
            case BigintType _ -> writer.bigint((Long) value.value());
            case IntegerType _ -> writer.integerValue((Long) value.value());
            case SmallintType _ -> writer.smallintValue((Long) value.value());
            case TinyintType _ -> writer.tinyintValue((Long) value.value());
            case DoubleType _ -> writer.doubleValue((Double) value.value());
            case RealType _ -> writer.realBits(((Long) value.value()).intValue());
            case VarcharType _, CharType _ -> writer.varchar((Slice) value.value());
            case DecimalType decimalType -> {
                if (decimalType.isShort()) {
                    writer.shortDecimal(decimalType.getPrecision(), decimalType.getScale(), (Long) value.value());
                }
                else {
                    writer.longDecimal(decimalType.getPrecision(), decimalType.getScale(), (Int128) value.value());
                }
            }
            case NumberType _ -> writer.numberValue((TrinoNumber) value.value());
            default -> throw new IllegalArgumentException("Unsupported scalar type for tree encoding: " + type);
        }
    }
}
