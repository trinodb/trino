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
package io.trino.plugin.clickhouse;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestClickHouseMaps
{
    @Test
    void testIsStringMap()
    {
        assertThat(ClickHouseMaps.isStringMap("Map(String, String)")).isTrue();
        assertThat(ClickHouseMaps.isStringMap("Map(LowCardinality(String), String)")).isTrue();
        assertThat(ClickHouseMaps.isStringMap("Map(String, Nullable(String))")).isTrue();
        assertThat(ClickHouseMaps.isStringMap("Map(String, UInt64)")).isFalse();
        assertThat(ClickHouseMaps.isStringMap("Map(UInt8, String)")).isFalse();
        assertThat(ClickHouseMaps.isStringMap("Map(FixedString(3), String)")).isFalse();
        assertThat(ClickHouseMaps.isStringMap("String")).isFalse();
        assertThat(ClickHouseMaps.isStringMap("Nested(")).isFalse();
    }
}
