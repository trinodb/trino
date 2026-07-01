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

import io.trino.spi.type.TypeId;
import org.junit.jupiter.api.Test;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestPersistedIntervalTypes
{
    @Test
    void testLegacyTypes()
    {
        assertThat(TESTING_TYPE_MANAGER.getType(TypeId.of("interval day to second")))
                .isEqualTo(TESTING_TYPE_MANAGER.fromSqlType("interval day(9) to second(3)"));
        assertThat(TESTING_TYPE_MANAGER.fromPersistedSqlType("interval year to month"))
                .isEqualTo(TESTING_TYPE_MANAGER.fromSqlType("interval year(9) to month"));
        assertThat(TESTING_TYPE_MANAGER.fromPersistedSqlType("row(x array(interval day to second), y interval year to month)"))
                .isEqualTo(TESTING_TYPE_MANAGER.fromSqlType("row(x array(interval day(9) to second(3)), y interval year(9) to month)"));
    }

    @Test
    void testNewTypeDefaultsAndExplicitPrecision()
    {
        assertThat(TESTING_TYPE_MANAGER.fromSqlType("interval day to second").getDisplayName())
                .isEqualTo("interval day(2) to second(6)");
        for (String type : new String[] {"interval day(2) to second(6)", "interval day(9) to second(12)", "interval year(2) to month", "interval second(2, 0)"}) {
            assertThat(TESTING_TYPE_MANAGER.fromPersistedSqlType(type))
                    .isEqualTo(TESTING_TYPE_MANAGER.fromSqlType(type));
        }
    }
}
