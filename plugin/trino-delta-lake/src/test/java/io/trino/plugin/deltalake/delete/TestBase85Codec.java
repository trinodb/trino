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
package io.trino.plugin.deltalake.delete;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.trino.plugin.deltalake.delete.Base85Codec.decodeUUID;
import static io.trino.plugin.deltalake.delete.Base85Codec.encodeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestBase85Codec
{
    @Test
    void testEncodeUUID()
    {
        assertThat(encodeUUID(UUID.fromString("a52eda8c-0a57-4636-814b-9c165388f7ca"))).isEqualTo("R7QFX3rGXPFLhHGq&7g<");
        assertThat(encodeUUID(UUID.fromString("d2c639aa-8816-431a-aaf6-d3fe2512ff61"))).isEqualTo("^-aqEH.-t@S}K{vb[*k^");
        assertThat(encodeUUID(UUID.fromString("00000000-0000-0000-0000-000000000000"))).isEqualTo("00000000000000000000");
    }

    @Test
    void testDecodeUUID()
    {
        assertThat(decodeUUID("R7QFX3rGXPFLhHGq&7g<")).isEqualTo(UUID.fromString("a52eda8c-0a57-4636-814b-9c165388f7ca"));
        assertThat(decodeUUID("00000000000000000000")).isEqualTo(UUID.fromString("00000000-0000-0000-0000-000000000000"));
    }

    @Test
    void testRoundTrip()
    {
        for (UUID uuid : new UUID[] {
                UUID.fromString("a52eda8c-0a57-4636-814b-9c165388f7ca"),
                UUID.fromString("d2c639aa-8816-431a-aaf6-d3fe2512ff61"),
                UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                new UUID(Long.MIN_VALUE, Long.MAX_VALUE),
        }) {
            String encoded = encodeUUID(uuid);
            assertThat(encoded).hasSize(Base85Codec.ENCODED_UUID_LENGTH);
            assertThat(decodeUUID(encoded)).isEqualTo(uuid);
        }
    }

    @Test
    void testDecodeInvalidInput()
    {
        // 'ä' is not part of the Z85 character set
        assertThatThrownBy(() -> decodeUUID("äbcdefghijklmnopqrst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Input is not valid Z85");
        // ',' is ASCII but not part of the Z85 character set
        assertThatThrownBy(() -> decodeUUID(",bcdefghijklmnopqrst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Input is not valid Z85");
        // input must be 5 character aligned
        assertThatThrownBy(() -> decodeUUID("abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("5 character aligned");
    }
}
