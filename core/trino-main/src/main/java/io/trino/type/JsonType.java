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
package io.trino.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.json.Json;
import io.trino.json.JsonBlock;
import io.trino.json.JsonBlockBuilder;
import io.trino.json.JsonItemEncoding;
import io.trino.json.JsonItemSemantics;
import io.trino.json.JsonItems;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.type.AbstractType;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VariableWidthType;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Optional;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

/// Trino's public JSON type. Backed by [JsonBlock], a flat variable-width block: one
/// `Slice` of concatenated per-row payloads, an offsets table, and a null flag per row,
/// like the other variable-width types. Writes copy the value's bytes into the block's
/// buffer — a value that already holds bytes (a connector read, or a path result whose
/// encoding was materialized) is copied verbatim, and a tree value is encoded straight
/// into the buffer — so no `Json` references are retained. A read reconstitutes a [Json]
/// view over the row's slice on demand.
///
/// Per-row representation lives inside [Json] itself. An ingested byte payload is
/// `EncodedJson` (typed-encoded or raw-text mode, discriminated by the leading byte); a
/// path-engine result stays as `JsonObject`/`JsonArray`/`TypedValue` and materializes its
/// byte form only when something needs bytes (block storage, network serde, equality
/// byte-compare).
public class JsonType
        extends AbstractType
        implements VariableWidthType
{
    public static final String NAME = "json";
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION =
            extractOperatorDeclaration(JsonType.class, lookup(), Json.class);

    // Flat-format encoding mirrors AbstractVariableWidthType: length stored big-endian
    // in the fixed-size slice; values <= MAX_SHORT_FLAT_LENGTH bytes are inlined.
    private static final VarHandle INT_BE_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final int MAX_SHORT_FLAT_LENGTH = 3;

    public static final JsonType JSON = new JsonType();

    private JsonType()
    {
        super(new TypeDescriptor(NAME), Json.class, JsonBlock.class);
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public int getFlatFixedSize()
    {
        return Integer.BYTES;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return true;
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            return 0;
        }
        return length;
    }

    @Override
    public JsonBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new JsonBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public JsonBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new JsonBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        Json value = readJson(block, position);
        if (value.isRawText()) {
            return value.rawText().toStringUtf8();
        }
        return JsonItems.toText(value).toStringUtf8();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        Json value = readJson(block, position);
        if (value.isRawText()) {
            return value.rawText();
        }
        return JsonItems.toText(value);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return readJson(block, position);
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        Json value = readJson(block, position);
        int length;
        if (value.isRawText()) {
            // Raw text needs to be parsed and re-encoded for flat storage so the on-disk
            // form matches what writeFlatFromStack/readFlat expect.
            length = JsonItems.fromText(value.rawText()).encoding().length();
        }
        else {
            length = value.encoding().length();
        }
        return length <= MAX_SHORT_FLAT_LENGTH ? 0 : length;
    }

    public void writeString(BlockBuilder blockBuilder, String value)
    {
        writeSlice(blockBuilder, Slices.utf8Slice(value));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        // Wrap the row's bytes as a Json and append it; the builder copies the bytes into
        // its buffer. wrapAsJson only reads the leading byte to tell raw text from encoded.
        Slice wrapped = (offset == 0 && length == value.length()) ? value : value.slice(offset, length);
        ((JsonBlockBuilder) blockBuilder).appendJson(wrapAsJson(wrapped));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        // Append the value; the builder copies its bytes into the buffer — an already-encoded
        // or raw-text value verbatim, a tree value (path-engine output) encoded on the way in.
        ((JsonBlockBuilder) blockBuilder).appendJson((Json) value);
    }

    /// Builds a block directly over a column of JSON values a reader already holds as one buffer
    /// plus an offsets table — which is this block's own layout. The buffer is adopted, not copied,
    /// and raw text is never parsed or re-encoded to get in.
    public static JsonBlock adopt(int positionCount, Optional<boolean[]> valueIsNull, int[] offsets, Slice buffer)
    {
        return new JsonBlock(positionCount, valueIsNull, offsets, buffer);
    }

    /// Wraps a slice (typed-encoding bytes or raw JSON text — distinguished by the
    /// leading byte) as a [Json] without copying. Used at the connector ingestion
    /// boundary by [#writeSlice].
    public static Json wrapAsJson(Slice slice)
    {
        return Json.wrap(slice);
    }

    private static Json readJson(Block block, int position)
    {
        JsonBlock jsonBlock = (JsonBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return jsonBlock.getJson(valuePosition);
    }

    /// Renders a JSON value (in either typed-item binary encoding or raw text form) to
    /// canonical JSON text. Exposed for callers that consume an asJson-style payload
    /// outside the block API (e.g. cross-module reflective tests).
    public static Slice jsonText(Slice slice)
    {
        return JsonItemEncoding.isEncoding(slice) ? JsonItems.toText(Json.of(slice)) : slice;
    }

    private static int readVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        int length = (int) INT_BE_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        if (length < 0) {
            int shortLength = fixedSizeSlice[fixedSizeOffset] & 0x7F;
            if (shortLength > MAX_SHORT_FLAT_LENGTH) {
                throw new IllegalArgumentException("Invalid short variable width length: " + shortLength);
            }
            return shortLength;
        }
        return length;
    }

    private static void writeFlatVariableLength(int length, byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        if (length < 0) {
            throw new IllegalArgumentException("Invalid variable width length: " + length);
        }
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            fixedSizeSlice[fixedSizeOffset] = (byte) (length | 0x80);
        }
        else {
            INT_BE_HANDLE.set(fixedSizeSlice, fixedSizeOffset, length);
        }
    }

    private static Json fromFlat(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
        byte[] bytes;
        int offset;
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            bytes = fixedSizeSlice;
            offset = fixedSizeOffset + 1;
        }
        else {
            bytes = variableSizeSlice;
            offset = variableSizeOffset;
        }
        Slice slice = Slices.wrappedBuffer(bytes, offset, length);
        return wrapAsJson(slice);
    }

    @ScalarOperator(READ_VALUE)
    private static Json readBlock(@BlockPosition JsonBlock block, @BlockIndex int position)
    {
        return block.getJson(position);
    }

    @ScalarOperator(READ_VALUE)
    private static Json readFlatToStack(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        return fromFlat(fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset,
            BlockBuilder blockBuilder)
    {
        ((JsonBlockBuilder) blockBuilder).appendJson(fromFlat(fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset));
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlatFromStack(
            Json value,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        // Must agree with getFlatVariableWidthSize and writeFlatFromBlock —
        // a raw-text value's `value.encoding()` (tree-encoded) is not
        // byte-equal to `JsonItems.fromText(rawText).encoding()` (stream-
        // encoded) once an INDEXED container shows up, and the size
        // reservation uses the stream form.
        Slice encoding;
        if (value.isRawText()) {
            encoding = JsonItems.fromText(value.rawText()).encoding();
        }
        else {
            encoding = value.encoding();
        }
        int length = encoding.length();
        writeFlatVariableLength(length, fixedSizeSlice, fixedSizeOffset);
        byte[] bytes;
        int offset;
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            bytes = fixedSizeSlice;
            offset = fixedSizeOffset + 1;
        }
        else {
            bytes = variableSizeSlice;
            offset = variableSizeOffset;
        }
        encoding.getBytes(0, bytes, offset, length);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlatFromBlock(
            @BlockPosition JsonBlock block,
            @BlockIndex int position,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        Json value = block.getJson(position);
        // Materialize tree / raw-text forms to typed encoding so flat storage stays
        // canonical for round-trip with writeFlatFromStack and readFlat.
        Slice encoded;
        if (value.isRawText()) {
            encoded = JsonItems.fromText(value.rawText()).encoding();
        }
        else {
            encoded = value.encoding();
        }
        int length = encoded.length();
        writeFlatVariableLength(length, fixedSizeSlice, fixedSizeOffset);
        byte[] bytes;
        int offset;
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            bytes = fixedSizeSlice;
            offset = fixedSizeOffset + 1;
        }
        else {
            bytes = variableSizeSlice;
            offset = variableSizeOffset;
        }
        encoded.getBytes(0, bytes, offset, length);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Json left, Json right)
    {
        return JsonItemSemantics.equal(left, right);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition JsonBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition JsonBlock rightBlock, @BlockIndex int rightPosition)
    {
        return JsonItemSemantics.equal(leftBlock.getJson(leftPosition), rightBlock.getJson(rightPosition));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Json left, @BlockPosition JsonBlock rightBlock, @BlockIndex int rightPosition)
    {
        return JsonItemSemantics.equal(left, rightBlock.getJson(rightPosition));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition JsonBlock leftBlock, @BlockIndex int leftPosition, Json right)
    {
        return JsonItemSemantics.equal(leftBlock.getJson(leftPosition), right);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        return JsonItemSemantics.equal(
                fromFlat(leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset),
                fromFlat(rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(
            @BlockPosition JsonBlock leftBlock,
            @BlockIndex int leftPosition,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        return JsonItemSemantics.equal(
                leftBlock.getJson(leftPosition),
                fromFlat(rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            @BlockPosition JsonBlock rightBlock,
            @BlockIndex int rightPosition)
    {
        return JsonItemSemantics.equal(
                fromFlat(leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset),
                rightBlock.getJson(rightPosition));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(
            Json left,
            @FlatFixed byte[] rightFixedSizeSlice,
            @FlatFixedOffset int rightFixedSizeOffset,
            @FlatVariableWidth byte[] rightVariableSizeSlice,
            @FlatVariableOffset int rightVariableSizeOffset)
    {
        return JsonItemSemantics.equal(
                left,
                fromFlat(rightFixedSizeSlice, rightFixedSizeOffset, rightVariableSizeSlice, rightVariableSizeOffset));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] leftVariableSizeSlice,
            @FlatVariableOffset int leftVariableSizeOffset,
            Json right)
    {
        return JsonItemSemantics.equal(
                fromFlat(leftFixedSizeSlice, leftFixedSizeOffset, leftVariableSizeSlice, leftVariableSizeOffset),
                right);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Json value)
    {
        return JsonItemSemantics.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition JsonBlock block, @BlockIndex int position)
    {
        return JsonItemSemantics.hash(block.getJson(position));
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        return JsonItemSemantics.hash(fromFlat(fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset));
    }
}
