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
package io.trino.type;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static io.trino.type.TypeCalculation.calculateLiteralValue;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTypeCalculation
{
    @Test
    public void testBasicUsage()
    {
        assertThat(calculateLiteralValue("42", ImmutableMap.of())).isEqualTo(Long.valueOf(42));
        assertThat(calculateLiteralValue("NULL", ImmutableMap.of())).isEqualTo(Long.valueOf(0));
        assertThat(calculateLiteralValue("null", ImmutableMap.of())).isEqualTo(Long.valueOf(0));
        assertThat(calculateLiteralValue("x", ImmutableMap.of("x", 42L))).isEqualTo(Long.valueOf(42));
        assertThat(calculateLiteralValue("(42)", ImmutableMap.of())).isEqualTo(Long.valueOf(42));
        assertThat(calculateLiteralValue("(NULL)", ImmutableMap.of())).isEqualTo(Long.valueOf(0));
        assertThat(calculateLiteralValue("(x)", ImmutableMap.of("x", 42L))).isEqualTo(Long.valueOf(42));

        assertThat(calculateLiteralValue("42 + 55", ImmutableMap.of())).isEqualTo(Long.valueOf(42 + 55));
        assertThat(calculateLiteralValue("42 - 55", ImmutableMap.of())).isEqualTo(Long.valueOf(42 - 55));
        assertThat(calculateLiteralValue("42 * 55", ImmutableMap.of())).isEqualTo(Long.valueOf(42 * 55));
        assertThat(calculateLiteralValue("42 / 6", ImmutableMap.of())).isEqualTo(Long.valueOf(42 / 6));

        assertThat(calculateLiteralValue("42 + 55 * 6", ImmutableMap.of())).isEqualTo(Long.valueOf(42 + 55 * 6));
        assertThat(calculateLiteralValue("(42 + 55) * 6", ImmutableMap.of())).isEqualTo(Long.valueOf((42 + 55) * 6));

        assertThat(calculateLiteralValue("min(10,2)", ImmutableMap.of())).isEqualTo(Long.valueOf(2));
        assertThat(calculateLiteralValue("min(10,2*10)", ImmutableMap.of())).isEqualTo(Long.valueOf(10));
        assertThat(calculateLiteralValue("max(10,2*10)", ImmutableMap.of())).isEqualTo(Long.valueOf(20));
        assertThat(calculateLiteralValue("max(10,2)", ImmutableMap.of())).isEqualTo(Long.valueOf(10));

        assertThat(calculateLiteralValue("x + y", ImmutableMap.of("x", 42L, "y", 55L))).isEqualTo(Long.valueOf(42 + 55));
    }
}
