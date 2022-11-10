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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayContainsSequence
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testBasic()
    {
        assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, 4, 5, 6]", "ARRAY[1, 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, 4, 5, 6]", "ARRAY[3, 4]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, 4, 5, 6]", "ARRAY[5, 6]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, 4, 5, 6]", "ARRAY[1, 2, 4]"))
                .isEqualTo(false);

        assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, NULL, 4, 5, 6]", "ARRAY[3, NULL, 4]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, 4, 5, 6]", "ARRAY[1, 2, 3, 4, 5, 6]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, 4, 5, 6]", "ARRAY[]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY['1', '2', '3']", "ARRAY['1', '2']"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[1.1, 2.2, 3.3]", "ARRAY[1.1, 2.2]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[ARRAY[1,2], ARRAY[3], ARRAY[4,5]]", "ARRAY[ARRAY[1,2], ARRAY[3]]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains_sequence", "ARRAY[ARRAY[1,2], ARRAY[3], ARRAY[4,5]]", "ARRAY[ARRAY[1,2], ARRAY[4]]"))
                .isEqualTo(false);

        for (int i = 1; i <= 6; i++) {
            assertThat(assertions.function("contains_sequence", "ARRAY[1, 2, 3, 4, 5, 6]", "ARRAY[%d]".formatted(i)))
                    .isEqualTo(true);
        }
    }
}
