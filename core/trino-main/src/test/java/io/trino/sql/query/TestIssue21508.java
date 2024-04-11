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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIssue21508
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query(
                    """
                    WITH t(k) AS (SELECT 1 WHERE random() >= 0)
                    SELECT *
                    FROM t
                    WHERE k IN (
                        SELECT k
                        FROM t
                        WHERE cardinality((VALUES empty_approx_set())) = 0)
                    """))
                    .matches("VALUES 1");
        }
    }
}
