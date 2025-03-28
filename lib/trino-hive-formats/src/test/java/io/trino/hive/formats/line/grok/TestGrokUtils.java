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
package io.trino.hive.formats.line.grok;

import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.hive.formats.FormatTestUtils.assertColumnValueEquals;
import static io.trino.hive.formats.FormatTestUtils.createLineBuffer;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGrokUtils
{
    private TestGrokUtils()
    {
    }

    public static void assertError(List<Column> columns, String line, Optional<String> inputFormat, Optional<String> inputGrokCustomPatterns,
                                    Optional<String> grokStrictMode, Optional<String> grokNamedOnlyMode, Optional<String> grokNullOnParseError,
                                    String errorMessage, Optional<Class> expectedErrorClass)
    {
        ThrowableAssert.ThrowingCallable testAction = () -> readLine(columns, line, inputFormat, inputGrokCustomPatterns, grokStrictMode, grokNamedOnlyMode, grokNullOnParseError);

        AbstractThrowableAssert<?, ? extends Throwable> assertion = assertThatThrownBy(testAction).hasMessage(errorMessage);

        expectedErrorClass.ifPresent(assertion::hasRootCauseInstanceOf);
    }

    public static void assertLine(List<Column> columns, String line, String inputFormat, Optional<String> inputGrokCustomPatterns,
                                   Optional<String> grokStrictMode, Optional<String> grokNamedOnlyMode, Optional<String> grokNullOnParseError, List<Object> expectedValues)
            throws IOException
    {
        List<Object> actualValues = readLine(columns, line, Optional.of(inputFormat), inputGrokCustomPatterns, grokStrictMode, grokNamedOnlyMode, grokNullOnParseError);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
    }

    public static List<Object> readLine(List<Column> columns, String line, Optional<String> inputFormat, Optional<String> inputGrokCustomPatterns,
                                         Optional<String> grokStrictMode, Optional<String> grokNamedOnlyMode, Optional<String> grokNullOnParseError)
            throws IOException
    {
        LineDeserializer deserializer = new GrokDeserializerFactory().create(columns, createGrokProperties(inputFormat, inputGrokCustomPatterns, grokStrictMode, grokNamedOnlyMode, grokNullOnParseError));
        PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
        deserializer.deserialize(createLineBuffer(line), pageBuilder);
        return readTrinoValues(columns, pageBuilder.build(), 0);
    }

    private static Map<String, String> createGrokProperties(Optional<String> inputFormat, Optional<String> inputGrokCustomPatterns,
                                                            Optional<String> grokStrictMode, Optional<String> grokNamedOnlyMode, Optional<String> grokNullOnParseError)
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();
        inputFormat.ifPresent(grokInputFormat -> schema.put(GrokDeserializerFactory.INPUT_FORMAT, grokInputFormat));
        inputGrokCustomPatterns.ifPresent(grokCustomPattern -> schema.put(GrokDeserializerFactory.INPUT_GROK_CUSTOM_PATTERNS, grokCustomPattern));
        grokStrictMode.ifPresent(strictMode -> schema.put(GrokDeserializerFactory.GROK_STRICT_MODE, strictMode));
        grokNamedOnlyMode.ifPresent(namedOnlyMode -> schema.put(GrokDeserializerFactory.GROK_NAMED_ONLY_MODE, namedOnlyMode));
        grokNullOnParseError.ifPresent(nullOnParseError -> schema.put(GrokDeserializerFactory.GROK_NULL_ON_PARSE_ERROR, nullOnParseError));
        return schema.buildOrThrow();
    }
}
