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
package io.trino.parquet.spark;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVariant
{
    @Test
    void testVariantToJson()
    {
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 12, 1}, new byte[] {1, 1, 0, 3, 99, 111, 108}, "{\"col\":1}");
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 12, 2}, new byte[] {1, 1, 0, 5, 97, 114, 114, 97, 121}, "{\"array\":2}");
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 12, 3}, new byte[] {1, 1, 0, 3, 109, 97, 112}, "{\"map\":3}");
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 12, 4}, new byte[] {1, 1, 0, 6, 115, 116, 114, 117, 99, 116}, "{\"struct\":4}");

        assertVariantToJson(new byte[] {12, 1}, new byte[] {1, 0, 0}, "1");
        assertVariantToJson(new byte[] {12, 2}, new byte[] {1, 0, 0}, "2");
        assertVariantToJson(new byte[] {12, 3}, new byte[] {1, 0, 0}, "3");
        assertVariantToJson(new byte[] {12, 4}, new byte[] {1, 0, 0}, "4");
    }

    @Test
    void testVariantToJsonAllTypes()
    {
        assertVariantToJson(new byte[] {4}, new byte[] {1, 0, 0}, "true");
        assertVariantToJson(new byte[] {12, 1}, new byte[] {1, 0, 0}, "1");
        assertVariantToJson(new byte[] {56, -51, -52, 76, 62}, new byte[] {1, 0, 0}, "0.2");
        assertVariantToJson(new byte[] {28, 51, 51, 51, 51, 51, 51, -45, 63}, new byte[] {1, 0, 0}, "0.3");
        assertVariantToJson(new byte[] {32, 1, 4, 0, 0, 0}, new byte[] {1, 0, 0}, "0.4");
        assertVariantToJson(new byte[] {37, 116, 101, 115, 116, 32, 100, 97, 116, 97}, new byte[] {1, 0, 0}, "\"test data\"");
        assertVariantToJson(new byte[] {60, 3, 0, 0, 0, 101, 104, 63}, new byte[] {1, 0, 0}, "\"ZWg/\"");
        assertVariantToJson(new byte[] {44, -27, 72, 0, 0}, new byte[] {1, 0, 0}, "\"2021-02-03\"");
        assertVariantToJson(new byte[] {48, -88, -34, 22, -20, 19, -116, 3, 0}, new byte[] {1, 0, 0}, "\"2001-08-22 01:02:03.321+00:00\"");
        assertVariantToJson(new byte[] {52, 64, -34, -104, 21, -22, -73, 5, 0}, new byte[] {1, 0, 0}, "\"2021-01-02 12:34:56.123456\"");
        assertVariantToJson(new byte[] {3, 1, 0, 2, 12, 1}, new byte[] {1, 0, 0}, "[1]");
        assertVariantToJson(new byte[] {2, 2, 0, 1, 0, 2, 4, 12, 1, 12, 2}, new byte[] {1, 2, 0, 4, 8, 107, 101, 121, 49, 107, 101, 121, 50}, "{\"key1\":1,\"key2\":2}");
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 12, 1}, new byte[] {1, 1, 0, 1, 120}, "{\"x\":1}");
    }

    @Test
    void testVariantToJsonTimestamp()
    {
        assertThat(new Variant(new byte[] {48, -88, -34, 22, -20, 19, -116, 3, 0}, new byte[] {1, 0, 0}).toJson(ZoneOffset.ofHours(-18)))
                .isEqualTo("\"2001-08-21 07:02:03.321-18:00\"");
        assertThat(new Variant(new byte[] {48, -88, -34, 22, -20, 19, -116, 3, 0}, new byte[] {1, 0, 0}).toJson(ZoneOffset.ofHours(-1)))
                .isEqualTo("\"2001-08-22 00:02:03.321-01:00\"");
        assertThat(new Variant(new byte[] {48, -88, -34, 22, -20, 19, -116, 3, 0}, new byte[] {1, 0, 0}).toJson(UTC))
                .isEqualTo("\"2001-08-22 01:02:03.321+00:00\"");
        assertThat(new Variant(new byte[] {48, -88, -34, 22, -20, 19, -116, 3, 0}, new byte[] {1, 0, 0}).toJson(ZoneOffset.ofHours(1)))
                .isEqualTo("\"2001-08-22 02:02:03.321+01:00\"");
        assertThat(new Variant(new byte[] {48, -88, -34, 22, -20, 19, -116, 3, 0}, new byte[] {1, 0, 0}).toJson(ZoneOffset.ofHours(18)))
                .isEqualTo("\"2001-08-22 19:02:03.321+18:00\"");
    }

    @Test
    void testVariantToJsonNullValue()
    {
        assertVariantToJson(new byte[] {2, 1, 0, 0, 1, 0}, new byte[] {1, 1, 0, 3, 99, 111, 108}, "{\"col\":null}");
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 0}, new byte[] {1, 1, 0, 5, 97, 114, 114, 97, 121}, "{\"array\":null}");
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 0}, new byte[] {1, 1, 0, 3, 109, 97, 112}, "{\"map\":null}");
        assertVariantToJson(new byte[] {2, 1, 0, 0, 2, 0}, new byte[] {1, 1, 0, 6, 115, 116, 114, 117, 99, 116}, "{\"struct\":null}");
    }

    private static void assertVariantToJson(byte[] value, byte[] metadata, @Language("JSON") String expectedJson)
    {
        assertThat(new Variant(value, metadata).toJson(UTC))
                .isEqualTo(expectedJson);
    }
}
