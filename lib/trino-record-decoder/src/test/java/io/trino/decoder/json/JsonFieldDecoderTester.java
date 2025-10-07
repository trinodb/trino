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
package io.trino.decoder.json;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.DecoderTestColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderSpec;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.decoder.util.DecoderTestUtil.TESTING_SESSION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonFieldDecoderTester
{
    private static final JsonRowDecoderFactory DECODER_FACTORY = new JsonRowDecoderFactory(new ObjectMapperProvider().get());

    private final Optional<String> dataFormat;
    private final Optional<String> formatHint;

    public JsonFieldDecoderTester()
    {
        this(Optional.empty(), Optional.empty());
    }

    public JsonFieldDecoderTester(String dataFormat)
    {
        this(Optional.of(dataFormat), Optional.empty());
    }

    public JsonFieldDecoderTester(String dataFormat, String formatHint)
    {
        this(Optional.of(dataFormat), Optional.of(formatHint));
    }

    private JsonFieldDecoderTester(Optional<String> dataFormat, Optional<String> formatHint)
    {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.formatHint = requireNonNull(formatHint, "formatHint is null");
    }

    public void assertDecodedAs(String jsonValue, Type type, long expectedValue)
    {
        checkArgument(type.getJavaType() == long.class, "Wrong (not long based) Trino type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertThat(decodedValue.isNull())
                .describedAs(format("expected non null when decoding %s as %s", jsonValue, type))
                .isFalse();
        assertThat(decodedValue.getLong()).isEqualTo(expectedValue);
    }

    public void assertDecodedAs(String jsonValue, Type type, double expectedValue)
    {
        checkArgument(type.getJavaType() == double.class, "Wrong (not double based) Trino type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertThat(decodedValue.isNull())
                .describedAs(format("expected non null when decoding %s as %s", jsonValue, type))
                .isFalse();
        assertThat(decodedValue.getDouble()).isEqualTo(expectedValue);
    }

    public void assertDecodedAs(String jsonValue, Type type, Slice expectedValue)
    {
        checkArgument(type.getJavaType() == Slice.class, "Wrong (not Slice based) Trino type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertThat(decodedValue.isNull())
                .describedAs(format("expected non null when decoding %s as %s", jsonValue, type))
                .isFalse();
        assertThat(decodedValue.getSlice()).isEqualTo(expectedValue);
    }

    public void assertDecodedAs(String jsonValue, Type type, boolean expectedValue)
    {
        checkArgument(type.getJavaType() == boolean.class, "Wrong (not boolean based) Trino type '%s'", type);
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertThat(decodedValue.isNull())
                .describedAs(format("expected non null when decoding %s as %s", jsonValue, type))
                .isFalse();
        assertThat(decodedValue.getBoolean()).isEqualTo(expectedValue);
    }

    public void assertDecodedAsNull(String jsonValue, Type type)
    {
        FieldValueProvider decodedValue = decode(Optional.of(jsonValue), type);
        assertThat(decodedValue.isNull())
                .describedAs(format("expected null when decoding %s as %s", jsonValue, type))
                .isTrue();
    }

    public void assertMissingDecodedAsNull(Type type)
    {
        FieldValueProvider decodedValue = decode(Optional.empty(), type);
        assertThat(decodedValue.isNull())
                .describedAs(format("expected null when decoding missing field as %s", type))
                .isTrue();
    }

    public void assertInvalidInput(String jsonValue, Type type, String exceptionRegex)
    {
        assertThatThrownBy(() -> decode(Optional.of(jsonValue), type).getLong())
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching(exceptionRegex);
    }

    private FieldValueProvider decode(Optional<String> jsonValue, Type type)
    {
        String jsonField = "value";
        String json = jsonValue.map(value -> format("{\"%s\":%s}", jsonField, value)).orElse("{}");
        DecoderTestColumnHandle columnHandle = new DecoderTestColumnHandle(
                0,
                "some_column",
                type,
                jsonField,
                dataFormat.orElse(null),
                formatHint.orElse(null),
                false,
                false,
                false);

        RowDecoder rowDecoder = DECODER_FACTORY.create(TESTING_SESSION, new RowDecoderSpec(JsonRowDecoder.NAME, emptyMap(), ImmutableSet.of(columnHandle)));
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json.getBytes(UTF_8))
                .orElseThrow(AssertionError::new);
        assertThat(decodedRow).containsKey(columnHandle);
        return decodedRow.get(columnHandle);
    }
}
