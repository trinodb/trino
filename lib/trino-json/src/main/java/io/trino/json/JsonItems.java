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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.primitives.Shorts;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.json.JsonItemBuilder.JsonItemWriter;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
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
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

/// Adapters between Jackson [JsonNode] trees and the typed-item [Json] encoding, used where a
/// value arrives from a Jackson-based path (`JsonMapper.readTree`) and has to cross into the
/// value model.
///
/// [#fromJsonNode] walks a Jackson tree and emits the typed-item encoding, discriminating
/// numbers per node type exactly as
/// `SqlJsonLiteralConverter#getNumericTypedValue` does: an `IntNode` becomes a
/// `TYPED_VALUE` with an `INTEGER` tag, a `LongNode` a `BIGINT`, a `DecimalNode` a
/// `DECIMAL(precision, scale)`.
public final class JsonItems
{
    /// Decimal-form JSON numbers parse as BigDecimal rather than Double so the SQL/JSON
    /// path engine can preserve precision (small values land in `DECIMAL`, oversized
    /// values promote to `NUMBER` via [#fromJsonNode]). Without this flag Jackson would
    /// silently round high-precision decimals to the nearest Double. Disabling
    /// `STRIP_TRAILING_BIGDECIMAL_ZEROES` keeps `1.0` from collapsing to `1` — the
    /// trailing zero is part of the source-side scale that the cast and round-trip
    /// paths rely on for byte-faithful output.
    // Trino columns can carry arbitrarily large strings and arbitrary-precision numbers;
    // Jackson's defaults (~5 MB string, 1000-digit number) would reject SQL-valid inputs,
    // so those caps are lifted to match the policy used by TrinoJsonCodec.
    //
    // Nesting depth is capped intentionally: parseTreeItem recurses on container depth,
    // and uncapped depth would let a hostile JSON column (e.g. ingested from an external
    // connector) crash the worker with a StackOverflowError. 1024 is comfortably above
    // any reasonable SQL workload and well below the JVM's default stack budget.
    private static final int MAX_NESTING_DEPTH = 1024;

    private static final JsonFactory JSON_FACTORY = buildJsonFactory();

    @SuppressModernizer
    // JsonFactoryBuilder usage is intentional to set custom stream-read constraints
    // (uncapped string/number length to admit large SQL values, with a hard nesting cap).
    private static JsonFactory buildJsonFactory()
    {
        return new JsonFactoryBuilder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxStringLength(Integer.MAX_VALUE)
                        .maxNestingDepth(MAX_NESTING_DEPTH)
                        .maxNumberLength(Integer.MAX_VALUE)
                        .build())
                .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                .enable(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                .build();
    }

    private static final JsonMapper MAPPER = JsonMapper.builder(JSON_FACTORY)
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false)
            .build();

    private JsonItems() {}

    /// Walks a Jackson [JsonNode] tree and returns its typed-item [Json] encoding.
    /// Numeric values that don't fit `BIGINT`/`DECIMAL(38)` are promoted to the
    /// arbitrary-precision NUMBER type rather than rejected.
    public static Json fromJsonNode(JsonNode node)
    {
        return JsonItemBuilder.encode(writer -> writeNode(writer, node));
    }

    /// Parses raw JSON text into a typed [Json] encoding. Streams via Jackson's
    /// token API directly into a [JsonItemBuilder.JsonItemWriter] so duplicate object
    /// keys are preserved as separate entries (Jackson's `ObjectNode` would collapse
    /// them, breaking ROW-to-JSON casts that emit empty-name fields). Throws a
    /// [TrinoException] with `INVALID_FUNCTION_ARGUMENT` on malformed input.
    public static Json fromText(Slice text)
    {
        // Pass the slice's raw bytes (not toStringUtf8()) so Jackson uses
        // UTF8StreamJsonParser. That avoids decoding the input bytes to chars and
        // re-encoding field names back to UTF-8 when we write them, and keeps key
        // canonicalization on `BytesToNameCanonicalizer` (byte hash) instead of
        // `CharsToNameCanonicalizer` (char hash).
        try (JsonParser parser = MAPPER.getFactory().createParser((InputStream) text.getInput())) {
            return parseStream(parser);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: " + e.getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: " + e.getMessage(), e);
        }
    }

    /// Streaming-parse from an [InputStream] — Jackson auto-detects UTF-8/16/32 and
    /// uses the byte-mode parser variant. Preferred entry point for callers whose
    /// input already exists as bytes (the common JSON-from-VARCHAR/VARBINARY case).
    public static Json fromText(InputStream input)
            throws JsonProcessingException, IOException
    {
        try (JsonParser parser = MAPPER.getFactory().createParser(input)) {
            return parseStream(parser);
        }
    }

    /// Same streaming-parse contract as [#fromText] but consumes a [Reader] —
    /// avoids buffering the input as a single `String` for callers that already have an
    /// encoding-specific [Reader] (e.g. JSON_VALUE / JSON_QUERY / JSON_OBJECT).
    public static Json fromText(Reader reader)
            throws JsonProcessingException, IOException
    {
        try (JsonParser parser = MAPPER.getFactory().createParser(reader)) {
            return parseStream(parser);
        }
    }

    /// Parses raw JSON text into a tree-form [Json]. Distinct from [#fromText]
    /// which produces the byte-encoded form: tree-form values get O(1)
    /// HashMap-indexed object lookup, pay the byte encoding cost lazily only
    /// when [Json#encoding] is first called, and preserve type fidelity via
    /// [TypedValue] scalars without an encode/decode round trip.
    public static Json parseToTree(Slice text)
    {
        try (JsonParser parser = MAPPER.getFactory().createParser((InputStream) text.getInput())) {
            return parseTreeStream(parser);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: " + e.getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: " + e.getMessage(), e);
        }
    }

    /// Streaming tree parse from an [InputStream]; mirrors [#fromText] but
    /// emits a tree.
    public static Json parseToTree(InputStream input)
            throws JsonProcessingException, IOException
    {
        try (JsonParser parser = MAPPER.getFactory().createParser(input)) {
            return parseTreeStream(parser);
        }
    }

    /// Streaming tree parse from a [Reader]; mirrors [#fromText] but emits a
    /// tree. Used by JSON_VALUE / JSON_QUERY / JSON_OBJECT callers whose
    /// input arrives as text with an explicit charset.
    public static Json parseToTree(Reader reader)
            throws JsonProcessingException, IOException
    {
        try (JsonParser parser = MAPPER.getFactory().createParser(reader)) {
            return parseTreeStream(parser);
        }
    }

    private static Json parseStream(JsonParser parser)
            throws IOException
    {
        JsonToken token = parser.nextToken();
        if (token == null) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: empty input");
        }
        Json result = JsonItemBuilder.encode(writer -> {
            try {
                streamItem(parser, parser.currentToken(), writer);
            }
            catch (IOException e) {
                throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: " + e.getMessage(), e);
            }
        });
        if (parser.nextToken() != null) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: trailing data after top-level value");
        }
        return result;
    }

    private static Json parseTreeStream(JsonParser parser)
            throws IOException
    {
        JsonToken token = parser.nextToken();
        if (token == null) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: empty input");
        }
        Json root = parseTreeItem(parser, token);
        if (parser.nextToken() != null) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: trailing data after top-level value");
        }
        return root;
    }

    private static Json parseTreeItem(JsonParser parser, JsonToken token)
            throws IOException
    {
        return switch (token) {
            case VALUE_NULL -> JsonNullValue.INSTANCE;
            case VALUE_TRUE -> new TypedValue(BooleanType.BOOLEAN, true);
            case VALUE_FALSE -> new TypedValue(BooleanType.BOOLEAN, false);
            case VALUE_STRING -> new TypedValue(VarcharType.VARCHAR, utf8Slice(parser.getText()));
            case VALUE_NUMBER_INT -> parseInteger(parser);
            case VALUE_NUMBER_FLOAT -> parseDecimal(parser);
            case START_ARRAY -> {
                List<Json> elements = new ArrayList<>();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_ARRAY; next = parser.nextToken()) {
                    if (next == null) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: unexpected end of array");
                    }
                    elements.add(parseTreeItem(parser, next));
                }
                yield new JsonArray(elements);
            }
            case START_OBJECT -> {
                List<JsonObjectMember> members = new ArrayList<>();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_OBJECT; next = parser.nextToken()) {
                    if (next != JsonToken.FIELD_NAME) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: expected field name");
                    }
                    Slice key = utf8Slice(parser.currentName());
                    JsonToken valueToken = parser.nextToken();
                    if (valueToken == null) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: unexpected end of object");
                    }
                    members.add(new JsonObjectMember(key, parseTreeItem(parser, valueToken)));
                }
                yield new JsonObject(members);
            }
            default -> throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: unexpected token " + token);
        };
    }

    /// Walks a tree-form [Json] and emits its typed-item byte encoding via
    /// [JsonItemBuilder]. Inverse of [#parseToTree] — called by tree-backed
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
            case EncodedJson encoded -> {
                // Raw-text mode: recurse into the parsed tree rather than round-trip through
                // encoding(), which would allocate a fresh sub-buffer per row.
                if (encoded.isRawText()) {
                    writeTreeJson(writer, encoded.parsed());
                }
                else {
                    // Typed-encoded children: copy their bytes verbatim into the open buffer.
                    writer.nest(encoded);
                }
            }
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
        if (type == BooleanType.BOOLEAN) {
            writer.booleanValue((Boolean) value.value());
        }
        else if (type == BigintType.BIGINT) {
            writer.bigint((Long) value.value());
        }
        else if (type == IntegerType.INTEGER) {
            writer.integerValue((Long) value.value());
        }
        else if (type == SmallintType.SMALLINT) {
            writer.smallintValue((Long) value.value());
        }
        else if (type == TinyintType.TINYINT) {
            writer.tinyintValue((Long) value.value());
        }
        else if (type == DoubleType.DOUBLE) {
            writer.doubleValue((Double) value.value());
        }
        else if (type == RealType.REAL) {
            writer.realBits(((Long) value.value()).intValue());
        }
        else if (type instanceof VarcharType || type instanceof CharType) {
            writer.varchar((Slice) value.value());
        }
        else if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                writer.shortDecimal(decimalType.getPrecision(), decimalType.getScale(), (Long) value.value());
            }
            else {
                writer.longDecimal(decimalType.getPrecision(), decimalType.getScale(), (Int128) value.value());
            }
        }
        else if (type == NumberType.NUMBER) {
            writer.numberValue((TrinoNumber) value.value());
        }
        else {
            throw new IllegalArgumentException("Unsupported scalar type for tree encoding: " + type);
        }
    }

    /// Reads a VALUE_NUMBER_INT token directly into a [TypedValue], skipping
    /// Jackson's NumberNode allocation. Mirrors [#writeNumber]'s
    /// type-discrimination rules — keep the two in sync.
    private static TypedValue parseInteger(JsonParser parser)
            throws IOException
    {
        return switch (parser.getNumberType()) {
            case INT -> new TypedValue(IntegerType.INTEGER, (long) parser.getIntValue());
            case LONG -> new TypedValue(BigintType.BIGINT, parser.getLongValue());
            case BIG_INTEGER -> parseBigInteger(parser.getBigIntegerValue());
            default -> throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "unsupported JSON integer representation");
        };
    }

    private static TypedValue parseBigInteger(BigInteger value)
    {
        if (value.bitLength() < Integer.SIZE) {
            return new TypedValue(IntegerType.INTEGER, value.longValue());
        }
        if (value.bitLength() < Long.SIZE) {
            return new TypedValue(BigintType.BIGINT, value.longValue());
        }
        // Too wide for BIGINT, but an exact literal of up to 38 digits is a DECIMAL. Only
        // beyond that does the value need the arbitrary-precision NUMBER type.
        BigDecimal decimal = new BigDecimal(value);
        if (decimal.precision() <= MAX_PRECISION) {
            return new TypedValue(createDecimalType(decimal.precision(), 0), Int128.valueOf(value));
        }
        return new TypedValue(NumberType.NUMBER, TrinoNumber.from(decimal));
    }

    /// Reads a VALUE_NUMBER_FLOAT token into a [TypedValue]. SQL:2023 9.42 GR 4 makes a
    /// JSON number "the value of the `<signed numeric literal>` whose characters are
    /// identical to J", so the literal's form picks the type: an exponent makes it an
    /// approximate literal (DOUBLE), and without one it is exact (DECIMAL). Exact values
    /// that overflow `DECIMAL(38, s)` promote to the arbitrary-precision NUMBER type
    /// rather than being rejected.
    private static TypedValue parseDecimal(JsonParser parser)
            throws IOException
    {
        String text = parser.getText();
        if (text.indexOf('e') >= 0 || text.indexOf('E') >= 0) {
            return new TypedValue(DoubleType.DOUBLE, parser.getDoubleValue());
        }
        BigDecimal decimal = parser.getDecimalValue();
        int precision = decimal.precision();
        int scale = decimal.scale();
        if (precision > MAX_PRECISION || scale < 0 || scale > MAX_PRECISION) {
            return new TypedValue(NumberType.NUMBER, TrinoNumber.from(decimal));
        }
        int adjustedPrecision = Math.max(precision, scale);
        DecimalType type = createDecimalType(adjustedPrecision, scale);
        if (type.isShort()) {
            return new TypedValue(type, decimal.unscaledValue().longValue());
        }
        return new TypedValue(type, Int128.valueOf(decimal.unscaledValue()));
    }

    private static void streamItem(JsonParser parser, JsonToken token, JsonItemBuilder.JsonItemWriter writer)
            throws IOException
    {
        switch (token) {
            case VALUE_NULL -> writer.nullValue();
            case VALUE_TRUE -> writer.booleanValue(true);
            case VALUE_FALSE -> writer.booleanValue(false);
            case VALUE_STRING -> writer.varchar(utf8Slice(parser.getText()));
            // Dispatch to the same typed-value parsers used by parseTreeItem so the streaming
            // path doesn't pay a Jackson NumberNode allocation per scalar number.
            case VALUE_NUMBER_INT -> writeTypedValue(writer, parseInteger(parser));
            case VALUE_NUMBER_FLOAT -> writeTypedValue(writer, parseDecimal(parser));
            case START_ARRAY -> {
                writer.startArray();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_ARRAY; next = parser.nextToken()) {
                    if (next == null) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: unexpected end of array");
                    }
                    streamItem(parser, next, writer);
                }
                writer.endArray();
            }
            case START_OBJECT -> {
                writer.startObject();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_OBJECT; next = parser.nextToken()) {
                    if (next != JsonToken.FIELD_NAME) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: expected field name");
                    }
                    writer.fieldName(parser.currentName());
                    JsonToken valueToken = parser.nextToken();
                    if (valueToken == null) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: unexpected end of object");
                    }
                    streamItem(parser, valueToken, writer);
                }
                writer.endObject();
            }
            default -> throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "invalid JSON: unexpected token " + token);
        }
    }

    /// Renders a typed [Json] back to raw JSON text. Inverse of [#fromText]. Walks the
    /// typed encoding directly via [JsonItemEncoding#writeJson] rather than going through
    /// a [JsonNode] intermediate, so DECIMAL trailing zeros and arbitrary-precision NUMBER
    /// values render exactly without Jackson's BigDecimal-stringification quirks.
    /// Short-circuits for raw-text [Json] (produced by [Json#unchecked]) — those values
    /// hold the original text and skip the fromText / re-serialize round trip.
    public static Slice toText(Json json)
    {
        if (json.isRawText()) {
            return json.rawText();
        }
        try (StringWriter writer = new StringWriter();
                JsonGenerator generator = MAPPER.getFactory().createGenerator(writer)) {
            JsonItemEncoding.writeJson(json.backingSlice(), json.viewOffset(), generator);
            generator.flush();
            return utf8Slice(writer.toString());
        }
        catch (IOException e) {
            throw new IllegalStateException("failed to serialize JSON value", e);
        }
    }

    private static void writeNode(JsonItemWriter writer, JsonNode node)
    {
        switch (node.getNodeType()) {
            case NULL, MISSING -> writer.nullValue();
            case BOOLEAN -> writer.booleanValue(node.booleanValue());
            case STRING -> writer.varchar(utf8Slice(node.textValue()));
            case NUMBER -> writeNumber(writer, node);
            case ARRAY -> {
                int count = node.size();
                if (count >= JsonItemEncoding.INDEXED_CONTAINER_THRESHOLD) {
                    writer.startIndexedArray();
                    for (JsonNode element : node) {
                        writeNode(writer, element);
                    }
                    writer.endIndexedArray();
                }
                else {
                    writer.startArray();
                    for (JsonNode element : node) {
                        writeNode(writer, element);
                    }
                    writer.endArray();
                }
            }
            case OBJECT -> {
                int count = node.size();
                if (count >= JsonItemEncoding.INDEXED_CONTAINER_THRESHOLD
                        && count <= JsonItemEncoding.MAX_OBJECT_INDEXED_COUNT) {
                    writer.startIndexedObject();
                    node.properties().forEach(field -> {
                        writer.fieldName(field.getKey());
                        writeNode(writer, field.getValue());
                    });
                    writer.endIndexedObject();
                }
                else {
                    writer.startObject();
                    node.properties().forEach(field -> {
                        writer.fieldName(field.getKey());
                        writeNode(writer, field.getValue());
                    });
                    writer.endObject();
                }
            }
            default -> throw new IllegalArgumentException("Unsupported JSON node type: " + node.getNodeType());
        }
    }

    /// Materializes a [Json] back into a Jackson [JsonNode] tree. Inverse of
    /// [#fromJsonNode]: scalar tags map to the JsonNode subtype that the corresponding
    /// numeric path-engine literal would have selected (BIGINT → LongNode,
    /// INTEGER → IntNode, etc.). Used by IR plan-layer callers that still hand around
    /// JsonNode trees (e.g. `IrConstantJsonSequence` content). Throws on
    /// [Json.Kind#ERROR] — the path engine carries its error sentinel as a
    /// [JsonItemBuilder#JSON_ERROR] [Json] value, so an error result never needs to
    /// cross the JsonNode boundary.
    public static JsonNode toJsonNode(Json json)
    {
        return switch (json.kind()) {
            case NULL -> NullNode.getInstance();
            case ERROR -> throw new IllegalStateException("Cannot materialize JSON_ERROR as JsonNode");
            case ARRAY -> {
                ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
                json.forEachArrayElement(child -> array.add(toJsonNode(child)));
                yield array;
            }
            case OBJECT -> {
                Map<String, JsonNode> members = new LinkedHashMap<>();
                json.forEachObjectMember((key, value) -> members.put(key, toJsonNode(value)));
                yield new ObjectNode(JsonNodeFactory.instance, members);
            }
            case SCALAR -> scalarToJsonNode(json);
        };
    }

    private static JsonNode scalarToJsonNode(Json json)
    {
        TypedValue value = json.materializeScalar();
        return switch (json.scalarType()) {
            case BOOLEAN -> BooleanNode.valueOf(value.getBooleanValue());
            case VARCHAR -> TextNode.valueOf(((Slice) value.getObjectValue()).toStringUtf8());
            case BIGINT -> LongNode.valueOf(value.getLongValue());
            case INTEGER -> IntNode.valueOf(toIntExact(value.getLongValue()));
            case SMALLINT, TINYINT -> ShortNode.valueOf(Shorts.checkedCast(value.getLongValue()));
            case DOUBLE -> DoubleNode.valueOf(value.getDoubleValue());
            case REAL -> FloatNode.valueOf(intBitsToFloat(toIntExact(value.getLongValue())));
            case DECIMAL -> {
                DecimalType decimalType = (DecimalType) value.type();
                BigInteger unscaledValue = decimalType.isShort()
                        ? BigInteger.valueOf(value.getLongValue())
                        : ((Int128) value.getObjectValue()).toBigInteger();
                // A scale-0 decimal round-trips via BigIntegerNode so the parser sees
                // VALUE_NUMBER_INT (preserved as text) rather than VALUE_NUMBER_FLOAT, which
                // forces a Double conversion in Jackson and silently loses precision.
                yield decimalType.getScale() == 0
                        ? BigIntegerNode.valueOf(unscaledValue)
                        : DecimalNode.valueOf(new BigDecimal(unscaledValue, decimalType.getScale()));
            }
            case NUMBER -> {
                TrinoNumber number = (TrinoNumber) value.getObjectValue();
                yield switch (number.toBigDecimal()) {
                    // Integer-valued BigDecimals round-trip via BigIntegerNode so the parser sees
                    // VALUE_NUMBER_INT (preserved as text) rather than VALUE_NUMBER_FLOAT (which
                    // forces a Double conversion in Jackson and silently loses precision).
                    case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> decimal.scale() <= 0
                            ? BigIntegerNode.valueOf(decimal.toBigInteger())
                            : DecimalNode.valueOf(decimal);
                    // NaN / Infinity have no JSON number form; emit as JSON strings to preserve the value.
                    case TrinoNumber.NotANumber _ -> TextNode.valueOf("NaN");
                    case TrinoNumber.Infinity(boolean negative) -> TextNode.valueOf(negative ? "-Infinity" : "+Infinity");
                };
            }
        };
    }

    private static void writeNumber(JsonItemWriter writer, JsonNode node)
    {
        switch (node) {
            case BigIntegerNode _ -> {
                if (node.canConvertToInt()) {
                    writer.integerValue(node.longValue());
                }
                else if (node.canConvertToLong()) {
                    writer.bigint(node.longValue());
                }
                else {
                    // BigInteger that doesn't fit in long: promote to arbitrary-precision NUMBER.
                    writer.numberValue(TrinoNumber.from(new BigDecimal(node.bigIntegerValue())));
                }
            }
            case DecimalNode _ -> {
                BigDecimal decimal = node.decimalValue();
                int precision = decimal.precision();
                int scale = decimal.scale();
                // Trino DECIMAL requires 0 <= scale <= precision <= 38; if either fails, the value
                // promotes to the arbitrary-precision NUMBER type rather than rejecting the input.
                if (precision > MAX_PRECISION || scale < 0 || scale > MAX_PRECISION) {
                    writer.numberValue(TrinoNumber.from(decimal));
                    return;
                }
                int adjustedPrecision = Math.max(precision, scale);
                if (createDecimalType(adjustedPrecision, scale).isShort()) {
                    long unscaled = decimal.unscaledValue().longValue();
                    writer.shortDecimal(adjustedPrecision, scale, unscaled);
                }
                else {
                    writer.longDecimal(adjustedPrecision, scale, Int128.valueOf(decimal.unscaledValue()));
                }
            }
            case DoubleNode _ -> writer.doubleValue(node.doubleValue());
            case FloatNode _ -> writer.realBits(floatToRawIntBits(node.floatValue()));
            case IntNode _ -> writer.integerValue(node.longValue());
            case LongNode _ -> writer.bigint(node.longValue());
            case ShortNode _ -> writer.smallintValue(node.longValue());
            default -> throw new IllegalArgumentException("Unsupported JSON number node: " + node.getClass().getSimpleName());
        }
    }
}
