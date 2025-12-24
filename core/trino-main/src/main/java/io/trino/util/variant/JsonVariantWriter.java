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
package io.trino.util.variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.variant.Metadata;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

import static com.fasterxml.jackson.core.JsonTokenId.ID_FALSE;
import static com.fasterxml.jackson.core.JsonTokenId.ID_NULL;
import static com.fasterxml.jackson.core.JsonTokenId.ID_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonTokenId.ID_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonTokenId.ID_START_ARRAY;
import static com.fasterxml.jackson.core.JsonTokenId.ID_START_OBJECT;
import static com.fasterxml.jackson.core.JsonTokenId.ID_STRING;
import static com.fasterxml.jackson.core.JsonTokenId.ID_TRUE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BOOLEAN_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DOUBLE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_FLOAT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_INT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_LONG_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeBoolean;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal16;
import static io.trino.spi.variant.VariantEncoder.encodeDouble;
import static io.trino.spi.variant.VariantEncoder.encodeFloat;
import static io.trino.spi.variant.VariantEncoder.encodeInt;
import static io.trino.spi.variant.VariantEncoder.encodeLong;
import static io.trino.spi.variant.VariantEncoder.encodeString;
import static io.trino.spi.variant.VariantEncoder.encodedStringSize;
import static io.trino.util.variant.NullPlannedValue.NULL_PLANNED_VALUE;
import static java.util.Objects.requireNonNull;

final class JsonVariantWriter
        implements VariantWriter
{
    public static final JsonVariantWriter JSON_VARIANT_WRITER = new JsonVariantWriter();

    private static final JsonFactory JSON_FACTORY = io.trino.plugin.base.util.JsonUtils.jsonFactory();

    private JsonVariantWriter() {}

    @Override
    public PlannedValue plan(Metadata.Builder metadataBuilder, Object value)
    {
        if (value == null) {
            return NULL_PLANNED_VALUE;
        }

        Slice json = (Slice) value;

        try (InputStream input = json.getInput(); JsonParser parser = JSON_FACTORY.createParser(input)) {
            JsonToken token = parser.nextToken();
            if (token == null) {
                throw new IllegalArgumentException("Invalid JSON input for VARIANT: empty input");
            }

            PlannedValue planned = planValue(parser, token, metadataBuilder);

            // Ensure we consumed exactly one top-level value
            JsonToken trailing = parser.nextToken();
            if (trailing != null) {
                throw new IllegalArgumentException("Invalid JSON input for VARIANT: trailing data after top-level value");
            }

            return planned;
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON input for VARIANT", e);
        }
    }

    private static PlannedValue planValue(JsonParser parser, JsonToken token, Metadata.Builder metadataBuilder)
            throws IOException
    {
        return switch (token.id()) {
            case ID_NULL -> NULL_PLANNED_VALUE;
            case ID_TRUE -> new PlannedBooleanValue(true);
            case ID_FALSE -> new PlannedBooleanValue(false);
            case ID_STRING -> new PlannedStringValue(utf8Slice(parser.getText()));
            case ID_NUMBER_INT, ID_NUMBER_FLOAT -> switch (parser.getNumberType()) {
                case INT -> new PlannedIntValue(parser.getIntValue());
                case LONG -> new PlannedLongValue(parser.getLongValue());
                case BIG_INTEGER -> new PlannedDecimal16Value(parser.getBigIntegerValue(), 0);
                case FLOAT -> new PlannedFloatValue(parser.getFloatValue());
                case DOUBLE -> new PlannedDoubleValue(parser.getDoubleValue());
                case BIG_DECIMAL -> {
                    BigDecimal decimal = parser.getDecimalValue();
                    yield new PlannedDecimal16Value(decimal.unscaledValue(), decimal.scale());
                }
            };
            case ID_START_OBJECT -> planObject(parser, metadataBuilder);
            case ID_START_ARRAY -> planArray(parser, metadataBuilder);

            default -> throw new IllegalArgumentException("Unsupported JSON token for VARIANT: " + token);
        };
    }

    private static PlannedValue planObject(JsonParser parser, Metadata.Builder metadataBuilder)
            throws IOException
    {
        // JSON object field order in VARIANT is lexicographical by field name bytes (via fieldId ordering),
        // so we record provisional fieldIds and values now, and sort by final fieldIds at write time.
        //
        // Duplicate keys: fail (matches JSON -> MAP cast behavior).
        IntArrayList fieldIds = new IntArrayList();
        List<PlannedValue> values = new ArrayList<>();

        IntSet seeFields = new IntOpenHashSet();

        for (JsonToken token = parser.nextToken(); token != JsonToken.END_OBJECT; token = parser.nextToken()) {
            if (token == null) {
                throw new IllegalArgumentException("Invalid JSON input for VARIANT: unexpected EOF in object");
            }
            if (token != JsonToken.FIELD_NAME) {
                throw new IllegalArgumentException("Invalid JSON input for VARIANT: expected FIELD_NAME but got " + token);
            }

            String fieldName = parser.currentName();
            int provisionalFieldId = metadataBuilder.addFieldName(utf8Slice(fieldName));

            if (!seeFields.add(provisionalFieldId)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Duplicate map keys are not allowed");
            }

            JsonToken valueToken = parser.nextToken();
            if (valueToken == null) {
                throw new IllegalArgumentException("Invalid JSON input for VARIANT: unexpected EOF after field name");
            }

            PlannedValue plannedValue = planValue(parser, valueToken, metadataBuilder);

            fieldIds.add(provisionalFieldId);
            values.add(plannedValue);
        }

        return new PlannedObjectValue(fieldIds.toIntArray(), values.toArray(PlannedValue[]::new));
    }

    private static PlannedValue planArray(JsonParser parser, Metadata.Builder metadataBuilder)
            throws IOException
    {
        List<PlannedValue> elements = new ArrayList<>();
        for (JsonToken token = parser.nextToken(); token != JsonToken.END_ARRAY; token = parser.nextToken()) {
            if (token == null) {
                throw new IllegalArgumentException("Invalid JSON input for VARIANT: unexpected EOF in array");
            }
            elements.add(planValue(parser, token, metadataBuilder));
        }
        return new PlannedArrayValue(elements.toArray(PlannedValue[]::new));
    }

    private record PlannedBooleanValue(boolean value)
            implements PlannedValue
    {
        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int size()
        {
            return ENCODED_BOOLEAN_SIZE;
        }

        @Override
        public int write(Slice out, int offset)
        {
            return encodeBoolean(value, out, offset);
        }
    }

    private record PlannedIntValue(int value)
            implements PlannedValue
    {
        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int size()
        {
            return ENCODED_INT_SIZE;
        }

        @Override
        public int write(Slice out, int offset)
        {
            return encodeInt(value, out, offset);
        }
    }

    private record PlannedLongValue(long value)
            implements PlannedValue
    {
        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int size()
        {
            return ENCODED_LONG_SIZE;
        }

        @Override
        public int write(Slice out, int offset)
        {
            return encodeLong(value, out, offset);
        }
    }

    private record PlannedFloatValue(float value)
            implements PlannedValue
    {
        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int size()
        {
            return ENCODED_FLOAT_SIZE;
        }

        @Override
        public int write(Slice out, int offset)
        {
            return encodeFloat(value, out, offset);
        }
    }

    private record PlannedDoubleValue(double value)
            implements PlannedValue
    {
        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int size()
        {
            return ENCODED_DOUBLE_SIZE;
        }

        @Override
        public int write(Slice out, int offset)
        {
            return encodeDouble(value, out, offset);
        }
    }

    private record PlannedDecimal16Value(BigInteger unscaledValue, int scale)
            implements PlannedValue
    {
        private PlannedDecimal16Value(BigInteger unscaledValue, int scale)
        {
            this.unscaledValue = requireNonNull(unscaledValue, "unscaledValue is null");
            this.scale = scale;
        }

        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int size()
        {
            return ENCODED_DECIMAL16_SIZE;
        }

        @Override
        public int write(Slice out, int offset)
        {
            return encodeDecimal16(unscaledValue, scale, out, offset);
        }
    }

    private record PlannedStringValue(Slice value)
            implements PlannedValue
    {
        private PlannedStringValue(Slice value)
        {
            this.value = requireNonNull(value, "value is null");
        }

        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int size()
        {
            return encodedStringSize(value.length());
        }

        @Override
        public int write(Slice out, int offset)
        {
            return encodeString(value, out, offset);
        }
    }
}
