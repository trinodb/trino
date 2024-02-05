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
package io.trino.decoder.util;

import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

public final class DecoderTestUtil
{
    public static final ConnectorSession TESTING_SESSION = TestingConnectorSession.builder().build();

    private DecoderTestUtil() {}

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, Slice value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertThat(provider).isNotNull();
        assertThat(provider.getSlice()).isEqualTo(value);
    }

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, String value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertThat(provider).isNotNull();
        assertThat(provider.getSlice().toStringUtf8()).isEqualTo(value);
    }

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertThat(provider).isNotNull();
        assertThat(provider.getLong()).isEqualTo(value);
    }

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, double value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertThat(provider).isNotNull();
        assertThat(provider.getDouble()).isCloseTo(value, offset(0.0001));
    }

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, boolean value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertThat(provider).isNotNull();
        assertThat(provider.getBoolean()).isEqualTo(value);
    }

    public static void checkIsNull(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertThat(provider).isNotNull();
        assertThat(provider.isNull()).isTrue();
    }
}
