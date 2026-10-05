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
package io.trino.sql.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * NaN nested in an array or row is neither less than nor greater than another value.
 * A negated comparison of such values cannot be rewritten as the opposite comparison.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestNestedNaNComparisons
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testNegatedRowValueComparison()
    {
        assertThat(assertions.query(
                """
                SELECT id FROM (VALUES (1, nan()), (2, DOUBLE '0.1'), (3, DOUBLE '0.9')) t(id, score)
                WHERE NOT ((score, id) < (DOUBLE '0.5', 0))
                """))
                .matches("VALUES 1, 3");
        assertThat(assertions.query(
                """
                SELECT id FROM (VALUES (1, nan()), (2, DOUBLE '0.1'), (3, DOUBLE '0.9')) t(id, score)
                WHERE (score, id) NOT BETWEEN (DOUBLE '0.2', 0) AND (DOUBLE '0.8', 10)
                """))
                .matches("VALUES 1, 2, 3");
    }

    @Test
    public void testNegatedArrayComparison()
    {
        assertThat(assertions.query("SELECT id FROM (VALUES (1, ARRAY[nan()]), (2, ARRAY[DOUBLE '0.0']), (3, ARRAY[DOUBLE '2.0'])) t(id, a) WHERE NOT (a < ARRAY[DOUBLE '1.0'])"))
                .matches("VALUES 1, 3");
    }
}
