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
package io.trino.spooling.filesystem.encryption;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.trino.filesystem.encryption.EncryptionKey;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAzureEncryptionHeadersTranslator
{
    private static final EncryptionHeadersTranslator SSE = EncryptionHeadersTranslator.forScheme("abfs");

    @Test
    public void testKnownKey()
    {
        byte[] key = "TrinoWillFlyWithSpooledProtocol!".getBytes(UTF_8);
        EncryptionKey encryption = new EncryptionKey(key, "AES256");

        Map<String, List<String>> headers = SSE.createHeaders(encryption);
        assertThat(headers)
                .hasSize(3)
                .containsEntry("x-ms-encryption-key", List.of("VHJpbm9XaWxsRmx5V2l0aFNwb29sZWRQcm90b2NvbCE="))
                .containsEntry("x-ms-encryption-key-sha256", List.of("bXwXXQkzTJYdEN+cDvfUtOobMCc1kKoPVD6aVi1wb9A="))
                .containsEntry("x-ms-encryption-algorithm", List.of("AES256"));
    }

    @Test
    public void testRoundTrip()
    {
        EncryptionKey key = EncryptionKey.randomAes256();
        assertThat(SSE.extractKey(SSE.createHeaders(key))).isEqualTo(key);
    }

    @Test
    public void testRoundTripWithMixedCaseHeaders()
    {
        EncryptionKey key = EncryptionKey.randomAes256();
        Map<String, List<String>> headers = mixCase(SSE.createHeaders(key));
        assertThat(SSE.extractKey(headers)).isEqualTo(key);
    }

    @Test
    public void testThrowsOnInvalidChecksum()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "x-ms-encryption-key", List.of("VHJpbm9XaWxsRmx5V2l0aFNwb29sZWRQcm90b2NvbCE="),
                "x-ms-encryption-key-sha256", List.of("brokenchecksum"),
                "x-ms-encryption-algorithm", List.of("AES256"));

        assertThatThrownBy(() -> SSE.extractKey(headers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Key SHA256 checksum does not match");
    }

    private static Map<String, List<String>> mixCase(Map<String, List<String>> headers)
    {
        Iterator<Function<String, String>> iterator = Iterators.cycle(
                String::toUpperCase,
                value -> value.replaceFirst("x-ms-", "X-Ms-"),
                value -> value.replaceFirst("x-ms-encryption", "X-ms-Encryption"));

        return headers.entrySet()
                .stream()
                .collect(toImmutableMap(
                        entry -> iterator.next().apply(entry.getKey()),
                        Map.Entry::getValue));
    }
}
