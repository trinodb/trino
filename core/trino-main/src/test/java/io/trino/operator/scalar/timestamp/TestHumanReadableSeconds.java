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
package io.trino.operator.scalar.timestamp;

import io.trino.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestHumanReadableSeconds
        extends AbstractTestFunctions
{
    @Test
    public void testToHumanRedableSecondsFormat()
    {
        assertFunction("human_readable_seconds(0)", VARCHAR, "0 seconds");
        assertFunction("human_readable_seconds(1)", VARCHAR, "1 second");
        assertFunction("human_readable_seconds(60)", VARCHAR, "1 minute");
        assertFunction("human_readable_seconds(-60)", VARCHAR, "1 minute");
        assertFunction("human_readable_seconds(61)", VARCHAR, "1 minute, 1 second");
        assertFunction("human_readable_seconds(-61)", VARCHAR, "1 minute, 1 second");
        assertFunction("human_readable_seconds(535333.9513888889)", VARCHAR, "6 days, 4 hours, 42 minutes, 14 seconds");
        assertFunction("human_readable_seconds(535333.2513888889)", VARCHAR, "6 days, 4 hours, 42 minutes, 13 seconds");
        assertFunction("human_readable_seconds(56363463)", VARCHAR, "93 weeks, 1 day, 8 hours, 31 minutes, 3 seconds");
        assertFunction("human_readable_seconds(3660)", VARCHAR, "1 hour, 1 minute");
        assertFunction("human_readable_seconds(3601)", VARCHAR, "1 hour, 1 second");
        assertFunction("human_readable_seconds(8003)", VARCHAR, "2 hours, 13 minutes, 23 seconds");
        assertFunction("human_readable_seconds(NULL)", VARCHAR, null);
        // check for NaN
        assertInvalidFunction("human_readable_seconds(0.0E0 / 0.0E0)", "Invalid argument found: NaN");
        // check for infinity
        assertInvalidFunction("human_readable_seconds(1.0E0 / 0.0E0)", "Invalid argument found: Infinity");
    }
}
