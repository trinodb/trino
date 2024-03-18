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
package io.trino.plugin.varada.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrayUtilsTest
{
    private final int num = 50;

    @Test
    public void testLongfill()
    {
        long[] testArray = new long[num];
        int newVal = 5;
        ArrayUtils.longfill(testArray, (byte) newVal);
        for (long val : testArray) {
            assertThat(val).isEqualTo(newVal);
        }
    }

    @Test
    public void testLongfill_emptyArray()
    {
        long[] testArray = new long[0];
        int newVal = 5;
        ArrayUtils.longfill(testArray, (byte) newVal);
        assertThat(testArray).isEmpty();
    }

    @Test
    public void testIntfill()
    {
        int[] testArray = new int[num];
        int newVal = 5;
        ArrayUtils.intFill(testArray, (byte) newVal);
        for (long val : testArray) {
            assertThat(val).isEqualTo(newVal);
        }
    }

    @Test
    public void testIntfill_emptyArray()
    {
        int[] testArray = new int[0];
        int newVal = 5;
        ArrayUtils.intFill(testArray, (byte) newVal);
        assertThat(testArray).isEmpty();
    }
}
