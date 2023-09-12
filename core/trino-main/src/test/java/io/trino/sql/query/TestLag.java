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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLag
{
    @Test
    public void testNullOffset()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThatThrownBy(() -> assertions.query("""
                    SELECT lag(v, null) OVER (ORDER BY k)
                    FROM (VALUES (1, 10), (2, 20)) t(k, v)
                    """))
                    .hasMessageMatching("Offset must not be null");
        }
    }
}
