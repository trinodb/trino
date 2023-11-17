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
package io.trino.testing.datatype;

import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static io.trino.testing.datatype.DataType.timeDataType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDataType
{
    @Test
    public void testTimeDataType()
    {
        assertThat(timeDataType(0).toLiteral(LocalTime.of(23, 59, 59, 0))).isEqualTo("TIME '23:59:59'");
        assertThat(timeDataType(3).toLiteral(LocalTime.of(23, 59, 59, 999_000_000))).isEqualTo("TIME '23:59:59.999'");
        assertThat(timeDataType(6).toLiteral(LocalTime.of(23, 59, 59, 999_999_000))).isEqualTo("TIME '23:59:59.999999'");
    }
}
