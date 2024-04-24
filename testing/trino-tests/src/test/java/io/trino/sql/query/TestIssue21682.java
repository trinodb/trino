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

import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIssue21682
{
    /**
     * Regression test for <a href="https://github.com/trinodb/trino/issues/21682">#21682</a>
     */
    @Test
    public void test()
            throws Exception
    {
        try (QueryAssertions assertions = new QueryAssertions(DistributedQueryRunner.builder(TEST_SESSION).build())) {
            assertThat(assertions.query(
                    """
                    WITH FUNCTION replace_certain_characters(s varchar)
                        RETURNS VARCHAR
                        BEGIN
                            DECLARE i INT DEFAULT 1;
                            loop_label: LOOP
                                IF i = 10 THEN
                                    LEAVE loop_label;
                                END IF;
                                SET s = regexp_replace(s, ARRAY['b'][i]);
                                SET i = i + 1;
                            END LOOP;
                            RETURN s;
                        END
                    SELECT replace_certain_characters('abc')
                    """))
                    .failure().hasMessageMatching("Array subscript must be less than or equal to array length: 2 > 1");
        }
    }
}
