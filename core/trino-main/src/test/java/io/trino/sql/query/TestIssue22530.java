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

public class TestIssue22530
{
    @Test
    void test()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query(
                    """
                    WITH t(a) AS (VALUES 1, 2, null)
                    SELECT CASE
                      WHEN a = 1 THEN true
                      ELSE false
                      END
                    FROM t
                    """))
                    .matches("VALUES true, false, false");
        }

        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query(
                    """
                    WITH t(a) AS (VALUES 1, 2, null)
                    SELECT CASE
                      WHEN a = 1 THEN false
                      ELSE true
                      END
                    FROM t
                    """))
                    .matches("VALUES false, true, true");
        }
    }
}
