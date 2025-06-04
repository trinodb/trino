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

public class TestIssue22806
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query(
                    """
                    SELECT
                        a,
                        SUM(CASE WHEN b = 1 THEN c ELSE 0 END),
                        SUM(CASE WHEN b = 2 THEN 0 ELSE c END),
                        SUM(CASE WHEN b = 3 THEN 0 ELSE d END),
                        SUM(CASE WHEN b = 4 THEN d ELSE 0 END)
                    FROM (
                      VALUES
                        (1, 1, 1, 1)
                    ) t(a, b, c, d)
                    GROUP BY a
                    """))
                    .matches("VALUES  (1, BIGINT '1' , BIGINT '1', BIGINT '1', BIGINT '0')");
        }
    }
}
