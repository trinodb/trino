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
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTypeCalculation
{
    @Test
    public void testBasicUsage()
    {
        assertEquals(Long.valueOf(42), calculateLiteralValue("42", ImmutableMap.of()));
        assertEquals(Long.valueOf(0), calculateLiteralValue("NULL", ImmutableMap.of()));
        assertEquals(Long.valueOf(0), calculateLiteralValue("null", ImmutableMap.of()));
        assertEquals(Long.valueOf(42), calculateLiteralValue("x", ImmutableMap.of("x", 42L)));
        assertEquals(Long.valueOf(42), calculateLiteralValue("(42)", ImmutableMap.of()));
        assertEquals(Long.valueOf(0), calculateLiteralValue("(NULL)", ImmutableMap.of()));
        assertEquals(Long.valueOf(42), calculateLiteralValue("(x)", ImmutableMap.of("x", 42L)));

        assertEquals(Long.valueOf(42 + 55), calculateLiteralValue("42 + 55", ImmutableMap.of()));
        assertEquals(Long.valueOf(42 - 55), calculateLiteralValue("42 - 55", ImmutableMap.of()));
        assertEquals(Long.valueOf(42 * 55), calculateLiteralValue("42 * 55", ImmutableMap.of()));
        assertEquals(Long.valueOf(42 / 6), calculateLiteralValue("42 / 6", ImmutableMap.of()));

        assertEquals(Long.valueOf(42 + 55 * 6), calculateLiteralValue("42 + 55 * 6", ImmutableMap.of()));
        assertEquals(Long.valueOf((42 + 55) * 6), calculateLiteralValue("(42 + 55) * 6", ImmutableMap.of()));

        assertEquals(Long.valueOf(2), calculateLiteralValue("min(10,2)", ImmutableMap.of()));
        assertEquals(Long.valueOf(10), calculateLiteralValue("min(10,2*10)", ImmutableMap.of()));
        assertEquals(Long.valueOf(20), calculateLiteralValue("max(10,2*10)", ImmutableMap.of()));
        assertEquals(Long.valueOf(10), calculateLiteralValue("max(10,2)", ImmutableMap.of()));

        assertEquals(Long.valueOf(42 + 55), calculateLiteralValue("x + y", ImmutableMap.of("x", 42L, "y", 55L)));
    }
}
