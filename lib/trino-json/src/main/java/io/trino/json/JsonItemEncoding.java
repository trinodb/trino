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

import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.TrinoNumber.AsBigDecimal;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Math.toIntExact;

/// Binary encoding for SQL/JSON items.
///
/// Wire format (BNF). Uppercase tokens are byte literals defined by [ItemTag] / [TypeTag];
/// numeric primitives are little-endian unless marked otherwise.
///
/// ```
///   encoding              ::= version item
///   version               ::= 0xF3
///
///   item                  ::= json-error
///                           | json-null
///                           | array
///                           | array-indexed
///                           | object
///                           | object-indexed
///                           | typed-value
///
///   json-error            ::= JSON_ERROR
///   json-null             ::= JSON_NULL
///   array                 ::= ARRAY int32 int32 item{count}           -- size (incl. tag), count, items
///   array-indexed         ::= ARRAY_INDEXED int32 int32{count+1} item{count}
///                                                                     -- count, offsets table, items
///   object                ::= OBJECT int32 int32 (string item){count} -- size (incl. tag), count, entries
///   object-indexed        ::= OBJECT_INDEXED int32 uint16{count} int32{count+1} (string item){count}
///                                                                     -- count, sorting permutation,
///                                                                     -- entry offsets, count entries
///   typed-value           ::= TYPED_VALUE type-body
///
///   type-body             ::= boolean-body | varchar-body | integral-body | floating-body
///                           | decimal-body | number-body
///
///   boolean-body          ::= BOOLEAN byte                            -- 0 = false, 1 = true
///   varchar-body          ::= VARCHAR string
///   integral-body         ::= BIGINT int64 | INTEGER int32 | SMALLINT int16 | TINYINT byte
///   floating-body         ::= DOUBLE int64                            -- IEEE 754 bit pattern
///                           | REAL int32                              -- IEEE 754 bit pattern
///   decimal-body          ::= DECIMAL int32 int32 short-flag (int64 | int128)
///                                                                     -- precision, scale, isShort,
///                                                                     -- 0 → int64; 1 → int128 (big-endian)
///   number-body           ::= NUMBER number-kind [int32 int32 byte{length}]
///                                                                     -- kind; when finite: scale,
///                                                                     -- unscaled length, unscaled
///                                                                     -- BigInteger (2's-complement,
///                                                                     -- big-endian)
///
///   string                ::= int32 byte{length}                     -- length-prefixed UTF-8 bytes
///   short-flag            ::= byte                                    -- 0 = short form, 1 = long form
///   number-kind           ::= byte                                   -- 0 = finite, 1 = NaN,
///                                                                     -- 2 = +Infinity, 3 = -Infinity
/// ```
///
/// The version byte (`VERSION = 0xF3`) is chosen from the `0xF0..0xFF` range so it cannot
/// collide with any UTF-8 leading byte that could start a valid JSON document, allowing a
/// single byte to disambiguate between this encoding and raw JSON text in shared
/// `Slice`-typed payloads.
///
/// `size` on plain `ARRAY` and `OBJECT` is the total byte length of the container
/// including the tag byte and size word, so `itemEndOffset(container) == offset + size`
/// is O(1). Without it, traversal would have to walk every nested item to find the
/// container's end, which dominates the cost of [Json#objectMember] on plain objects.
/// The indexed forms already get O(1) end-offset from their offsets table and so don't
/// carry a redundant size word.
///
/// `ARRAY_INDEXED` and `OBJECT_INDEXED` are alternate forms emitted only when a container
/// has at least [#INDEXED_CONTAINER_THRESHOLD] entries; below the threshold the offsets
/// table is pure overhead. `OBJECT_INDEXED` additionally caps at
/// [#MAX_OBJECT_INDEXED_COUNT] entries (uint16 sort-permutation slots) and falls back to
/// plain `OBJECT` above that. The indexed forms support O(1) element access (array) and
/// O(log n) keyed lookup (object), while preserving insertion order for iteration.
///
/// Endianness: numeric fixed-width fields are little-endian via [SliceOutput]. The one
/// exception is [Int128], which is serialized via its canonical big-endian byte form to
/// match the layout used elsewhere in the SPI for decimal values.
public final class JsonItemEncoding
{
    public static final byte VERSION = (byte) 0xF3;

    /// Containers with at least this many entries are emitted in indexed form.
    public static final int INDEXED_CONTAINER_THRESHOLD = 8;

    /// Maximum entry count that fits in an [ItemTag#OBJECT_INDEXED] sort permutation.
    public static final int MAX_OBJECT_INDEXED_COUNT = 0xFFFF;

    public enum ItemTag
    {
        JSON_ERROR(1),
        JSON_NULL(2),
        ARRAY(3),
        OBJECT(4),
        TYPED_VALUE(5),
        ARRAY_INDEXED(6),
        OBJECT_INDEXED(7);

        private final byte encoded;

        ItemTag(int encoded)
        {
            this.encoded = (byte) encoded;
        }

        public byte encoded()
        {
            return encoded;
        }

        public static ItemTag fromEncoded(byte encoded)
        {
            return switch (encoded) {
                case 1 -> JSON_ERROR;
                case 2 -> JSON_NULL;
                case 3 -> ARRAY;
                case 4 -> OBJECT;
                case 5 -> TYPED_VALUE;
                case 6 -> ARRAY_INDEXED;
                case 7 -> OBJECT_INDEXED;
                default -> throw new IllegalArgumentException("Unsupported SQL/JSON item tag: " + encoded);
            };
        }
    }

    /// Type tags for [ItemTag#TYPED_VALUE] bodies. A character string always uses the VARCHAR
    /// tag -- CHAR is not a separate tag, since it carries no significant trailing spaces once
    /// in JSON. The gap at tag 3 is left reserved.
    public enum TypeTag
    {
        BOOLEAN(1),
        VARCHAR(2),
        BIGINT(4),
        INTEGER(5),
        SMALLINT(6),
        TINYINT(7),
        DOUBLE(8),
        REAL(9),
        DECIMAL(10),
        NUMBER(11);

        private final byte encoded;

        TypeTag(int encoded)
        {
            this.encoded = (byte) encoded;
        }

        public byte encoded()
        {
            return encoded;
        }

        public static TypeTag fromEncoded(byte encoded)
        {
            return switch (encoded) {
                case 1 -> BOOLEAN;
                case 2 -> VARCHAR;
                case 4 -> BIGINT;
                case 5 -> INTEGER;
                case 6 -> SMALLINT;
                case 7 -> TINYINT;
                case 8 -> DOUBLE;
                case 9 -> REAL;
                case 10 -> DECIMAL;
                case 11 -> NUMBER;
                default -> throw new IllegalArgumentException("Unsupported SQL/JSON typed value tag: " + encoded);
            };
        }
    }

    private JsonItemEncoding() {}

    // --- write side primitives -------------------------------------------------------

    public static void appendVersion(SliceOutput output)
    {
        output.appendByte(VERSION);
    }

    public static void appendJsonNullItem(SliceOutput output)
    {
        output.appendByte(ItemTag.JSON_NULL.encoded());
    }

    public static void appendJsonErrorItem(SliceOutput output)
    {
        output.appendByte(ItemTag.JSON_ERROR.encoded());
    }

    /// Width of the plain-container header: tag byte + size word + count word.
    /// `ARRAY` and `OBJECT` start their entries this many bytes past the tag offset.
    public static final int CONTAINER_HEADER_SIZE = Byte.BYTES + Integer.BYTES + Integer.BYTES;

    /// Byte offset of the size word relative to the container's tag offset.
    private static final int SIZE_OFFSET = Byte.BYTES;
    /// Byte offset of the count word relative to the container's tag offset.
    private static final int COUNT_OFFSET = Byte.BYTES + Integer.BYTES;

    /// Writes the header of an ARRAY item with deferred size and element count: ARRAY tag
    /// + 4 reserved bytes for the total byte size + 4 reserved bytes for the element count,
    /// both of which the caller must patch on close. Returns the absolute offset of the
    /// container's tag byte; size lives at `tagOffset + 1`, count at `tagOffset + 5`.
    public static int appendArrayItemPlaceholder(SliceOutput output)
    {
        int tagOffset = output.size();
        output.appendByte(ItemTag.ARRAY.encoded());
        output.appendInt(0); // size placeholder
        output.appendInt(0); // count placeholder
        return tagOffset;
    }

    /// Same as [#appendArrayItemPlaceholder] but for OBJECT items.
    public static int appendObjectItemPlaceholder(SliceOutput output)
    {
        int tagOffset = output.size();
        output.appendByte(ItemTag.OBJECT.encoded());
        output.appendInt(0); // size placeholder
        output.appendInt(0); // count placeholder
        return tagOffset;
    }

    /// Returns the offset of the size word for a container whose tag is at `tagOffset`.
    public static int containerSizeOffset(int tagOffset)
    {
        return tagOffset + SIZE_OFFSET;
    }

    /// Returns the offset of the count word for a container whose tag is at `tagOffset`.
    public static int containerCountOffset(int tagOffset)
    {
        return tagOffset + COUNT_OFFSET;
    }

    /// Writes a complete ARRAY_INDEXED item: tag + count + (count + 1) offsets table +
    /// item bytes. The offsets table holds the start offset of each element relative to
    /// the items-region start, with a sentinel offset at index `count` equal to the
    /// region length so consecutive-element distances and the past-end pointer are both
    /// O(1) lookups.
    public static void appendArrayIndexed(SliceOutput output, int[] offsets, Slice items)
    {
        int count = offsets.length - 1;
        output.appendByte(ItemTag.ARRAY_INDEXED.encoded());
        output.appendInt(count);
        for (int o : offsets) {
            output.appendInt(o);
        }
        output.writeBytes(items);
    }

    /// Writes a complete OBJECT_INDEXED item: tag + count + sort permutation
    /// (`count` uint16 entries giving the entry index appearing at each sorted position) +
    /// (count + 1) offsets table + entry bytes (each entry is a length-prefixed UTF-8
    /// key followed by an item, in entry/insertion order). The sort permutation lets
    /// `objectMember(key)` resolve in O(log n) via binary search on key bytes.
    public static void appendObjectIndexed(SliceOutput output, int count, short[] sortPermutation, int[] offsets, Slice entries)
    {
        if (sortPermutation.length != count) {
            throw new IllegalArgumentException("sortPermutation length must equal count");
        }
        if (offsets.length != count + 1) {
            throw new IllegalArgumentException("offsets length must be count + 1");
        }
        output.appendByte(ItemTag.OBJECT_INDEXED.encoded());
        output.appendInt(count);
        for (short s : sortPermutation) {
            output.appendShort(s);
        }
        for (int o : offsets) {
            output.appendInt(o);
        }
        output.writeBytes(entries);
    }

    public static void appendObjectKey(SliceOutput output, String key)
    {
        writeSlice(output, utf8Slice(key));
    }

    public static void appendBigint(SliceOutput output, long value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.BIGINT.encoded());
        output.appendLong(value);
    }

    public static void appendInteger(SliceOutput output, long value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.INTEGER.encoded());
        output.appendInt(toIntExact(value));
    }

    public static void appendSmallint(SliceOutput output, long value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.SMALLINT.encoded());
        output.appendShort((short) toIntExact(value));
    }

    public static void appendTinyint(SliceOutput output, long value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.TINYINT.encoded());
        output.appendByte((byte) toIntExact(value));
    }

    public static void appendDouble(SliceOutput output, double value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.DOUBLE.encoded());
        output.appendLong(doubleToRawLongBits(value));
    }

    public static void appendRealBits(SliceOutput output, int bits)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.REAL.encoded());
        output.appendInt(bits);
    }

    public static void appendBoolean(SliceOutput output, boolean value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.BOOLEAN.encoded());
        output.appendByte(value ? 1 : 0);
    }

    public static void appendVarchar(SliceOutput output, Slice value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.VARCHAR.encoded());
        writeSlice(output, value);
    }

    public static void appendShortDecimal(SliceOutput output, int precision, int scale, long value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.DECIMAL.encoded());
        output.appendInt(precision);
        output.appendInt(scale);
        output.appendByte(0);
        output.appendLong(value);
    }

    public static void appendLongDecimal(SliceOutput output, int precision, int scale, Int128 value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.DECIMAL.encoded());
        output.appendInt(precision);
        output.appendInt(scale);
        output.appendByte(1);
        output.writeBytes(value.toBigEndianBytes());
    }

    /// Encodes an arbitrary-precision NUMBER. Body layout:
    /// `kind:byte` then for [TrinoNumber.BigDecimalValue] only `scale:int32`,
    /// `byteLength:int32`, `bytes:byte[byteLength]` (BigInteger 2's complement
    /// big-endian). Kind is 0=BigDecimal, 1=NaN, 2=+Infinity, 3=-Infinity.
    public static void appendNumber(SliceOutput output, TrinoNumber value)
    {
        output.appendByte(ItemTag.TYPED_VALUE.encoded());
        output.appendByte(TypeTag.NUMBER.encoded());
        AsBigDecimal asBigDecimal = value.toBigDecimal();
        switch (asBigDecimal) {
            case TrinoNumber.NotANumber _ -> output.appendByte((byte) 1);
            case TrinoNumber.Infinity inf -> output.appendByte((byte) (inf.negative() ? 3 : 2));
            case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> {
                output.appendByte((byte) 0);
                output.appendInt(decimal.scale());
                byte[] bytes = decimal.unscaledValue().toByteArray();
                output.appendInt(bytes.length);
                output.writeBytes(bytes);
            }
        }
    }

    /// Embeds an existing typed-encoding payload as an item (the inner item bytes are
    /// copied, version byte stripped). Throws on the JSON_ERROR sentinel and on raw text
    /// (text→encoded conversion lives at a higher layer).
    public static void appendNestedItem(SliceOutput output, Slice payload)
    {
        if (!isEncoding(payload)) {
            throw new IllegalArgumentException("Expected typed-item encoded payload");
        }
        if (isJsonError(payload)) {
            throw new IllegalArgumentException("JSON_ERROR sentinel cannot be written as a JSON value");
        }
        output.writeBytes(payload, 1, payload.length() - 1);
    }

    private static void writeSlice(SliceOutput output, Slice value)
    {
        output.appendInt(value.length());
        output.writeBytes(value);
    }

    // --- read side: identification + offset traversal ------------------------------

    public static boolean isEncoding(Slice slice)
    {
        return slice.length() > 0 && slice.getByte(0) == VERSION;
    }

    public static boolean isJsonError(Slice slice)
    {
        return slice.length() == 2 && slice.getByte(0) == VERSION && slice.getByte(1) == ItemTag.JSON_ERROR.encoded();
    }

    /// Byte offset of the root item within an encoded payload (skips the version byte).
    public static int rootItemOffset(Slice slice)
    {
        if (!isEncoding(slice)) {
            throw new IllegalArgumentException("Unsupported SQL/JSON item encoding version: " + (slice.length() == 0 ? "<empty>" : slice.getByte(0)));
        }
        return 1;
    }

    public static ItemTag itemTag(Slice slice, int offset)
    {
        return ItemTag.fromEncoded(slice.getByte(offset));
    }

    public static int arraySize(Slice slice, int offset)
    {
        return switch (itemTag(slice, offset)) {
            case ARRAY -> {
                int count = slice.getInt(offset + COUNT_OFFSET);
                validateCount(count);
                yield count;
            }
            case ARRAY_INDEXED -> {
                int count = slice.getInt(offset + Byte.BYTES);
                validateCount(count);
                yield count;
            }
            default -> throw new IllegalArgumentException("Expected ARRAY item");
        };
    }

    /// Byte offset of the first element of an ARRAY-shaped item — past the size+count
    /// header and (if indexed) past the offsets table.
    public static int arrayItemsStart(Slice slice, int offset)
    {
        return switch (itemTag(slice, offset)) {
            case ARRAY -> offset + CONTAINER_HEADER_SIZE;
            case ARRAY_INDEXED -> {
                int count = slice.getInt(offset + Byte.BYTES);
                yield offset + Byte.BYTES + Integer.BYTES + (count + 1) * Integer.BYTES;
            }
            default -> throw new IllegalArgumentException("Expected ARRAY item");
        };
    }

    /// For an [ItemTag#ARRAY_INDEXED] item, returns the absolute byte offset of the
    /// element at `index`. The caller must ensure `0 <= index < arraySize(...)`.
    public static int arrayIndexedElementOffset(Slice slice, int offset, int index)
    {
        int offsetsTableStart = offset + Byte.BYTES + Integer.BYTES;
        int count = slice.getInt(offset + Byte.BYTES);
        return offset + Byte.BYTES + Integer.BYTES + (count + 1) * Integer.BYTES
                + slice.getInt(offsetsTableStart + index * Integer.BYTES);
    }

    public static int objectSize(Slice slice, int offset)
    {
        return switch (itemTag(slice, offset)) {
            case OBJECT -> {
                int count = slice.getInt(offset + COUNT_OFFSET);
                validateCount(count);
                yield count;
            }
            case OBJECT_INDEXED -> {
                int count = slice.getInt(offset + Byte.BYTES);
                validateCount(count);
                yield count;
            }
            default -> throw new IllegalArgumentException("Expected OBJECT item");
        };
    }

    /// Byte offset of the first entry of an OBJECT-shaped item — past the size+count
    /// header and (if indexed) past the sort permutation and offsets table.
    public static int objectEntriesStart(Slice slice, int offset)
    {
        return switch (itemTag(slice, offset)) {
            case OBJECT -> offset + CONTAINER_HEADER_SIZE;
            case OBJECT_INDEXED -> {
                int count = slice.getInt(offset + Byte.BYTES);
                int afterCount = offset + Byte.BYTES + Integer.BYTES;
                int afterPerm = afterCount + count * Short.BYTES;
                yield afterPerm + (count + 1) * Integer.BYTES;
            }
            default -> throw new IllegalArgumentException("Expected OBJECT item");
        };
    }

    public static int objectIndexedSorting(Slice slice, int offset, int sortedIndex)
    {
        int permStart = offset + Byte.BYTES + Integer.BYTES;
        return slice.getShort(permStart + sortedIndex * Short.BYTES) & 0xFFFF;
    }

    public static int objectIndexedEntryOffset(Slice slice, int offset, int entryIndex)
    {
        int count = slice.getInt(offset + Byte.BYTES);
        int permStart = offset + Byte.BYTES + Integer.BYTES;
        int offsetsStart = permStart + count * Short.BYTES;
        int entriesStart = offsetsStart + (count + 1) * Integer.BYTES;
        return entriesStart + slice.getInt(offsetsStart + entryIndex * Integer.BYTES);
    }

    public static int objectIndexedEntryEnd(Slice slice, int offset, int entryIndex)
    {
        int count = slice.getInt(offset + Byte.BYTES);
        int permStart = offset + Byte.BYTES + Integer.BYTES;
        int offsetsStart = permStart + count * Short.BYTES;
        int entriesStart = offsetsStart + (count + 1) * Integer.BYTES;
        return entriesStart + slice.getInt(offsetsStart + (entryIndex + 1) * Integer.BYTES);
    }

    /// Byte offset just past a length-prefixed string starting at `offset`.
    public static int stringEndOffset(Slice slice, int offset)
    {
        return offset + Integer.BYTES + slice.getInt(offset);
    }

    /// Reads a length-prefixed UTF-8 string starting at `offset` into a Java String.
    public static String readString(Slice slice, int offset)
    {
        return slice.slice(offset + Integer.BYTES, slice.getInt(offset)).toStringUtf8();
    }

    /// Returns a sub-slice for the length-prefixed string at `offset` (no copy).
    public static Slice readStringSlice(Slice slice, int offset)
    {
        return slice.slice(offset + Integer.BYTES, slice.getInt(offset));
    }

    /// Byte offset just past the item starting at `offset`.
    public static int itemEndOffset(Slice slice, int offset)
    {
        return switch (itemTag(slice, offset)) {
            case JSON_ERROR, JSON_NULL -> offset + Byte.BYTES;
            case ARRAY -> arrayEndOffset(slice, offset);
            case ARRAY_INDEXED -> arrayIndexedEndOffset(slice, offset);
            case OBJECT -> objectEndOffset(slice, offset);
            case OBJECT_INDEXED -> objectIndexedEndOffset(slice, offset);
            case TYPED_VALUE -> typedValueEndOffset(slice, offset + Byte.BYTES);
        };
    }

    private static int arrayEndOffset(Slice slice, int offset)
    {
        // Plain ARRAY: read the size word stamped at close time. O(1).
        return offset + slice.getInt(offset + SIZE_OFFSET);
    }

    private static int arrayIndexedEndOffset(Slice slice, int offset)
    {
        int count = slice.getInt(offset + Byte.BYTES);
        validateCount(count);
        int offsetsStart = offset + Byte.BYTES + Integer.BYTES;
        int itemsStart = offsetsStart + (count + 1) * Integer.BYTES;
        return itemsStart + slice.getInt(offsetsStart + count * Integer.BYTES);
    }

    private static int objectEndOffset(Slice slice, int offset)
    {
        // Plain OBJECT: read the size word stamped at close time. O(1).
        return offset + slice.getInt(offset + SIZE_OFFSET);
    }

    private static int objectIndexedEndOffset(Slice slice, int offset)
    {
        int count = slice.getInt(offset + Byte.BYTES);
        validateCount(count);
        int permStart = offset + Byte.BYTES + Integer.BYTES;
        int offsetsStart = permStart + count * Short.BYTES;
        int entriesStart = offsetsStart + (count + 1) * Integer.BYTES;
        return entriesStart + slice.getInt(offsetsStart + count * Integer.BYTES);
    }

    private static int typedValueEndOffset(Slice slice, int afterItemTag)
    {
        TypeTag typeTag = TypeTag.fromEncoded(slice.getByte(afterItemTag));
        int afterTypeTag = afterItemTag + Byte.BYTES;
        return switch (typeTag) {
            case BOOLEAN, TINYINT -> afterTypeTag + Byte.BYTES;
            case SMALLINT -> afterTypeTag + Short.BYTES;
            case INTEGER, REAL -> afterTypeTag + Integer.BYTES;
            case BIGINT, DOUBLE -> afterTypeTag + Long.BYTES;
            case VARCHAR -> stringEndOffset(slice, afterTypeTag);
            case DECIMAL -> {
                int afterScale = afterTypeTag + Integer.BYTES + Integer.BYTES;
                byte shortFlag = slice.getByte(afterScale);
                int afterFlag = afterScale + Byte.BYTES;
                yield shortFlag == 0 ? afterFlag + Long.BYTES : afterFlag + 16;
            }
            case NUMBER -> {
                byte kind = slice.getByte(afterTypeTag);
                int afterKind = afterTypeTag + Byte.BYTES;
                if (kind != 0) {
                    yield afterKind;
                }
                int afterScale = afterKind + Integer.BYTES;
                int byteLength = slice.getInt(afterScale);
                yield afterScale + Integer.BYTES + byteLength;
            }
        };
    }

    /// Decodes the [ItemTag#TYPED_VALUE] body at `itemOffset` into a [TypedValue].
    /// `itemOffset` must point to the TYPED_VALUE tag byte; the type tag and body
    /// follow.
    public static TypedValue readTypedValue(Slice slice, int itemOffset)
    {
        if (itemTag(slice, itemOffset) != ItemTag.TYPED_VALUE) {
            throw new IllegalArgumentException("Expected TYPED_VALUE item");
        }
        int bodyOffset = itemOffset + Byte.BYTES + Byte.BYTES;
        return switch (TypeTag.fromEncoded(slice.getByte(itemOffset + Byte.BYTES))) {
            case BOOLEAN -> new TypedValue(BOOLEAN, slice.getByte(bodyOffset) != 0);
            case TINYINT -> new TypedValue(TINYINT, (long) slice.getByte(bodyOffset));
            case SMALLINT -> new TypedValue(SMALLINT, (long) slice.getShort(bodyOffset));
            case INTEGER -> new TypedValue(INTEGER, (long) slice.getInt(bodyOffset));
            case BIGINT -> new TypedValue(BIGINT, slice.getLong(bodyOffset));
            case DOUBLE -> new TypedValue(DOUBLE, Double.longBitsToDouble(slice.getLong(bodyOffset)));
            case REAL -> new TypedValue(REAL, (long) slice.getInt(bodyOffset));
            case VARCHAR -> new TypedValue(VARCHAR, readStringSlice(slice, bodyOffset));
            case DECIMAL -> {
                int precision = slice.getInt(bodyOffset);
                int scale = slice.getInt(bodyOffset + Integer.BYTES);
                int afterScale = bodyOffset + Integer.BYTES + Integer.BYTES;
                byte shortFlag = slice.getByte(afterScale);
                int afterFlag = afterScale + Byte.BYTES;
                if (shortFlag == 0) {
                    yield new TypedValue(createDecimalType(precision, scale), slice.getLong(afterFlag));
                }
                byte[] bytes = new byte[16];
                slice.getBytes(afterFlag, bytes, 0, 16);
                yield new TypedValue(createDecimalType(precision, scale), Int128.fromBigEndian(bytes));
            }
            case NUMBER -> {
                byte kind = slice.getByte(bodyOffset);
                AsBigDecimal asBigDecimal = switch (kind) {
                    case 0 -> {
                        int scale = slice.getInt(bodyOffset + Byte.BYTES);
                        int byteLength = slice.getInt(bodyOffset + Byte.BYTES + Integer.BYTES);
                        byte[] bytes = new byte[byteLength];
                        slice.getBytes(bodyOffset + Byte.BYTES + Integer.BYTES + Integer.BYTES, bytes, 0, byteLength);
                        yield new TrinoNumber.BigDecimalValue(new BigDecimal(new BigInteger(bytes), scale));
                    }
                    case 1 -> new TrinoNumber.NotANumber();
                    case 2 -> new TrinoNumber.Infinity(false);
                    case 3 -> new TrinoNumber.Infinity(true);
                    default -> throw new IllegalArgumentException("Unsupported NUMBER kind: " + kind);
                };
                yield new TypedValue(NUMBER, TrinoNumber.from(asBigDecimal));
            }
        };
    }

    /// Returns a fresh, self-contained encoded slice for the item at `[itemOffset, endOffset)`
    /// inside `slice`. Prepends a fresh VERSION byte.
    ///
    /// Callers must treat the returned slice as immutable; the short-circuit path below
    /// shares the input's underlying byte array with the caller, so a mutation would leak
    /// to every other holder. Trino's [Slice] usage is read-only by convention, so this is
    /// safe in practice — but `Slice.toByteBuffer()` exposes a writable view, which would
    /// violate the contract.
    public static Slice copyItemEncoding(Slice slice, int itemOffset, int endOffset)
    {
        // Already-self-contained payload (root item at offset 1 with no trailing bytes) —
        // return the slice as-is. This is the hot path: tree-form `encoding()` calls go
        // through here on the slice returned by `JsonItemBuilder.encode`, which is by
        // construction a self-contained payload with VERSION at byte 0.
        if (itemOffset == 1 && endOffset == slice.length() && isEncoding(slice)) {
            return slice;
        }
        SliceOutput output = new DynamicSliceOutput(endOffset - itemOffset + 1);
        output.appendByte(VERSION);
        output.writeBytes(slice, itemOffset, endOffset - itemOffset);
        return output.slice();
    }

    /// Walks the typed-item encoding starting at `itemOffset` and emits JSON text via
    /// `generator`. Numeric scalars are written as digit-strings (via
    /// `BigDecimal.toPlainString` for DECIMAL / NUMBER) so trailing zeros and full
    /// precision survive a round-trip — Jackson's `writeNumber(BigDecimal)` would
    /// otherwise normalize away the original scale.
    public static void writeJson(Slice slice, int itemOffset, JsonGenerator generator)
            throws IOException
    {
        switch (itemTag(slice, itemOffset)) {
            case JSON_ERROR -> throw new IllegalArgumentException("JSON_ERROR cannot be rendered as JSON text");
            case JSON_NULL -> generator.writeNull();
            case ARRAY, ARRAY_INDEXED -> {
                int count = arraySize(slice, itemOffset);
                int cursor = arrayItemsStart(slice, itemOffset);
                generator.writeStartArray();
                for (int i = 0; i < count; i++) {
                    writeJson(slice, cursor, generator);
                    cursor = itemEndOffset(slice, cursor);
                }
                generator.writeEndArray();
            }
            case OBJECT, OBJECT_INDEXED -> {
                int count = objectSize(slice, itemOffset);
                int cursor = objectEntriesStart(slice, itemOffset);
                generator.writeStartObject();
                for (int i = 0; i < count; i++) {
                    generator.writeFieldName(readString(slice, cursor));
                    cursor = stringEndOffset(slice, cursor);
                    writeJson(slice, cursor, generator);
                    cursor = itemEndOffset(slice, cursor);
                }
                generator.writeEndObject();
            }
            case TYPED_VALUE -> writeTypedValueJson(slice, itemOffset + Byte.BYTES, generator);
        }
    }

    private static void writeTypedValueJson(Slice slice, int bodyOffset, JsonGenerator generator)
            throws IOException
    {
        TypeTag typeTag = TypeTag.fromEncoded(slice.getByte(bodyOffset));
        int payload = bodyOffset + Byte.BYTES;
        switch (typeTag) {
            case BOOLEAN -> generator.writeBoolean(slice.getByte(payload) != 0);
            case VARCHAR -> generator.writeString(readString(slice, payload));
            case BIGINT -> generator.writeNumber(slice.getLong(payload));
            case INTEGER -> generator.writeNumber(slice.getInt(payload));
            case SMALLINT -> generator.writeNumber(slice.getShort(payload));
            case TINYINT -> generator.writeNumber(slice.getByte(payload));
            case DOUBLE -> {
                double d = Double.longBitsToDouble(slice.getLong(payload));
                if (Double.isFinite(d)) {
                    generator.writeNumber(Double.toString(d));
                }
                else {
                    generator.writeString(Double.toString(d));
                }
            }
            case REAL -> {
                float f = Float.intBitsToFloat(slice.getInt(payload));
                if (Float.isFinite(f)) {
                    generator.writeNumber(Float.toString(f));
                }
                else {
                    generator.writeString(Float.toString(f));
                }
            }
            case DECIMAL -> {
                // payload layout: int32 precision (unused at render time) + int32 scale + byte longFlag + body
                int scale = slice.getInt(payload + Integer.BYTES);
                int longFlag = slice.getByte(payload + Integer.BYTES + Integer.BYTES);
                int unscaledStart = payload + Integer.BYTES + Integer.BYTES + Byte.BYTES;
                BigInteger unscaled;
                if (longFlag == 0) {
                    unscaled = BigInteger.valueOf(slice.getLong(unscaledStart));
                }
                else {
                    byte[] bytes = new byte[Int128.SIZE];
                    slice.getBytes(unscaledStart, bytes, 0, Int128.SIZE);
                    unscaled = Int128.fromBigEndian(bytes).toBigInteger();
                }
                // toPlainString preserves the scale (so DECIMAL(3,1) value 1.0 renders as
                // "1.0", not "1"); Jackson's writeNumber(BigDecimal) would otherwise route
                // through BigDecimal.toString and may normalize away the trailing zero.
                generator.writeNumber(new BigDecimal(unscaled, scale).toPlainString());
            }
            case NUMBER -> {
                byte kind = slice.getByte(payload);
                switch (kind) {
                    case 0 -> {
                        int scale = slice.getInt(payload + Byte.BYTES);
                        int byteLength = slice.getInt(payload + Byte.BYTES + Integer.BYTES);
                        byte[] bytes = new byte[byteLength];
                        slice.getBytes(payload + Byte.BYTES + Integer.BYTES + Integer.BYTES, bytes, 0, byteLength);
                        BigInteger unscaled = new BigInteger(bytes);
                        BigDecimal decimal = new BigDecimal(unscaled, scale);
                        // Render integer-magnitude values as digits and very-large-exponent
                        // values via BigDecimal.toString (which uses scientific for negative
                        // scales). The bit-length-plus-magnitude heuristic catches cases like
                        // 12345678901234567890 (after TrinoNumber strips its trailing zero,
                        // ends up with scale=-1) so the digit form survives the round-trip,
                        // while keeping 1e308 / 9.9e-324 in compact scientific form.
                        if (decimal.scale() == 0) {
                            generator.writeNumber(unscaled);
                        }
                        else if (decimal.scale() < 0 && unscaled.bitLength() - decimal.scale() <= 64) {
                            generator.writeNumber(decimal.toBigInteger());
                        }
                        else {
                            generator.writeNumber(decimal);
                        }
                    }
                    case 1 -> generator.writeString("NaN");
                    case 2 -> generator.writeString("+Infinity");
                    case 3 -> generator.writeString("-Infinity");
                    default -> throw new IllegalArgumentException("Unknown NUMBER encoding kind: " + kind);
                }
            }
        }
    }

    private static void validateCount(int count)
    {
        if (count < 0) {
            throw new IllegalArgumentException("Negative item count: " + count);
        }
    }
}
