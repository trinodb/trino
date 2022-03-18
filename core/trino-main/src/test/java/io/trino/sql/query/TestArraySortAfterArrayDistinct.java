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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArraySortAfterArrayDistinct
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
    public void testArrayDistinctAfterArraySort()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_SORT(items)) as result from (VALUES (ARRAY ['elephant', 'dog', 'cat', 'dog'])) t(items)"))
                .matches("VALUES (ARRAY  ['cat', 'dog', 'elephant'])");
    }

    @Test
    public void testArrayDistinctAfterArraySortWithLambda()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_SORT(items, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))) as result from (VALUES (ARRAY ['elephant', 'dog', 'cat', 'dog'])) t(items)"))
                .matches("VALUES (ARRAY  ['elephant', 'dog', 'cat'])");
    }
}
