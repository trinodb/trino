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
package io.trino.spi.transaction;

import org.junit.jupiter.api.Test;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.REPEATABLE_READ;
import static io.trino.spi.transaction.IsolationLevel.SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIsolationLevel
{
    @Test
    public void testMeetsRequirementOf()
    {
        assertThat(READ_UNCOMMITTED.meetsRequirementOf(READ_UNCOMMITTED)).isTrue();
        assertThat(READ_UNCOMMITTED.meetsRequirementOf(READ_COMMITTED)).isFalse();
        assertThat(READ_UNCOMMITTED.meetsRequirementOf(REPEATABLE_READ)).isFalse();
        assertThat(READ_UNCOMMITTED.meetsRequirementOf(SERIALIZABLE)).isFalse();

        assertThat(READ_COMMITTED.meetsRequirementOf(READ_UNCOMMITTED)).isTrue();
        assertThat(READ_COMMITTED.meetsRequirementOf(READ_COMMITTED)).isTrue();
        assertThat(READ_COMMITTED.meetsRequirementOf(REPEATABLE_READ)).isFalse();
        assertThat(READ_COMMITTED.meetsRequirementOf(SERIALIZABLE)).isFalse();

        assertThat(REPEATABLE_READ.meetsRequirementOf(READ_UNCOMMITTED)).isTrue();
        assertThat(REPEATABLE_READ.meetsRequirementOf(READ_COMMITTED)).isTrue();
        assertThat(REPEATABLE_READ.meetsRequirementOf(REPEATABLE_READ)).isTrue();
        assertThat(REPEATABLE_READ.meetsRequirementOf(SERIALIZABLE)).isFalse();

        assertThat(SERIALIZABLE.meetsRequirementOf(READ_UNCOMMITTED)).isTrue();
        assertThat(SERIALIZABLE.meetsRequirementOf(READ_COMMITTED)).isTrue();
        assertThat(SERIALIZABLE.meetsRequirementOf(REPEATABLE_READ)).isTrue();
        assertThat(SERIALIZABLE.meetsRequirementOf(SERIALIZABLE)).isTrue();
    }

    @Test
    public void testToString()
    {
        assertThat(READ_UNCOMMITTED.toString()).isEqualTo("READ UNCOMMITTED");
        assertThat(READ_COMMITTED.toString()).isEqualTo("READ COMMITTED");
        assertThat(REPEATABLE_READ.toString()).isEqualTo("REPEATABLE READ");
        assertThat(SERIALIZABLE.toString()).isEqualTo("SERIALIZABLE");
    }
}
