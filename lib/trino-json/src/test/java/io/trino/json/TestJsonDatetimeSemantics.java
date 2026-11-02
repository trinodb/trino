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

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static org.assertj.core.api.Assertions.assertThat;

class TestJsonDatetimeSemantics
{
    @Test
    void testStableHashVectors()
    {
        List<Json> values = List.of(
                JsonItemBuilder.encodeDate(1234),
                JsonItemBuilder.encodeTime(12, 1234),
                JsonItemBuilder.encodeTimeWithTimeZone(12, 1234, 0),
                JsonItemBuilder.encodeTimestamp(6, 1234, 0),
                JsonItemBuilder.encodeTimestampWithTimeZone(3, 1234, 0, UTC_KEY.getKey()));
        long[] expected = {39215, 40176, 41137, 42098, 43059};
        for (int i = 0; i < values.size(); i++) {
            assertThat(JsonItemSemantics.hash(values.get(i))).isEqualTo(expected[i]);
            assertThat(JsonItemSemantics.hash(values.get(i).materializeScalar())).isEqualTo(expected[i]);
            for (int j = i + 1; j < values.size(); j++) {
                assertThat(values.get(i)).isNotEqualTo(values.get(j));
            }
        }
        Json array = new JsonArray(values.subList(0, 2));
        assertThat(JsonItemSemantics.hash(array)).isEqualTo(1256802);
        assertThat(JsonItemSemantics.hash(Json.of(array.encoding()))).isEqualTo(1256802);
        assertThat(JsonItemSemantics.hash(JsonItemBuilder.encodeTimestamp(12, 1234, 567))).isEqualTo(42665);
    }

    @Test
    void testPrecisionAndZoneIndependentEquality()
    {
        assertEqual(JsonItemBuilder.encodeTime(0, 43_200_000_000_000_000L), JsonItemBuilder.encodeTime(12, 43_200_000_000_000_000L));
        // UTC normalization wraps to the previous day, across short and long stack forms.
        assertEqual(JsonItemBuilder.encodeTimeWithTimeZone(3, 1_800_000_000_000_000L, 60),
                JsonItemBuilder.encodeTimeWithTimeZone(12, 84_600_000_000_000_000L, 0));
        assertEqual(JsonItemBuilder.encodeTimestamp(6, -1_000_000, 0), JsonItemBuilder.encodeTimestamp(12, -1_000_000, 0));
        assertEqual(JsonItemBuilder.encodeTimestamp(9, 1234, 1000), JsonItemBuilder.encodeTimestamp(12, 1234, 1000));
        short losAngeles = getTimeZoneKey("America/Los_Angeles").getKey();
        assertEqual(JsonItemBuilder.encodeTimestampWithTimeZone(3, -1000, 0, UTC_KEY.getKey()),
                JsonItemBuilder.encodeTimestampWithTimeZone(12, -1000, 0, losAngeles));
        assertEqual(JsonItemBuilder.encodeTimestampWithTimeZone(6, 1234, 123_000_000, UTC_KEY.getKey()),
                JsonItemBuilder.encodeTimestampWithTimeZone(12, 1234, 123_000_000, losAngeles));
        assertThat(JsonItemBuilder.encodeTimestamp(12, 1234, 1000))
                .isNotEqualTo(JsonItemBuilder.encodeTimestamp(12, 1234, 1001));
        assertThat(JsonItemBuilder.encodeTimestampWithTimeZone(12, 1234, 1000, UTC_KEY.getKey()))
                .isNotEqualTo(JsonItemBuilder.encodeTimestampWithTimeZone(12, 1234, 1001, UTC_KEY.getKey()));
    }

    private static void assertEqual(Json left, Json right)
    {
        assertThat(left).isEqualTo(right);
        assertThat(JsonItemSemantics.hash(left)).isEqualTo(JsonItemSemantics.hash(right));
        assertThat(left.materializeScalar()).isEqualTo(right);
        assertThat(JsonItemSemantics.hash(left.materializeScalar())).isEqualTo(JsonItemSemantics.hash(right));
    }
}
