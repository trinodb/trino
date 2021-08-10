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
package io.trino.operator.scalar;

import org.testng.annotations.Test;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class TestArrayAverageFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("array_average(ARRAY [0.5E1, 0.7E1])", DOUBLE, 6d);
        assertFunction("array_average(ARRAY[5, 4])", DOUBLE, 4.5d);
        assertFunction("array_average(ARRAY[CAST(5 as BIGINT), 4])", DOUBLE, 4.5d);
        assertFunction("array_average(ARRAY[CAST(5 AS DECIMAL(7,2)), 4])", DOUBLE, 4.5d);
    }
}
