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
public class TestArrayAggregationAfterDistinct
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
    public void singleArrayDistinctArrayAgg()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_AGG(a)) FROM (VALUES (1), (1)) t(a)"))
                .matches("VALUES (ARRAY  [1])");
    }

    @Test
    public void singleArrayDistinctArrayAggAlreadyDistinct()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_AGG(DISTINCT a)) FROM (VALUES (1), (1)) t(a)"))
                .matches("VALUES (ARRAY  [1])");
    }

    @Test
    public void multipleArrayDistinctMultipleArrayAgg()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_AGG(a)), ARRAY_DISTINCT(ARRAY_AGG(DISTINCT b)) FROM (VALUES (1, 2), (1, 2)) t(a, b)"))
                .matches("VALUES (ARRAY [1], ARRAY [2])");
    }

    @Test
    public void multipleArrayDistinctSingleArrayAgg()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_AGG(a)), ARRAY_DISTINCT(ARRAY_AGG(a)) FROM (VALUES (1, 2), (1, 2)) t(a, b)"))
                .matches("VALUES (ARRAY [1], ARRAY [1])");
    }

    @Test
    public void multipleArrayDistinctArrayAggDifferentOrder()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_AGG(b)), ARRAY_DISTINCT(ARRAY_AGG(a)) FROM (VALUES (1, 2), (1, 2)) t(a, b)"))
                .matches("VALUES (ARRAY [2], ARRAY [1])");
    }

    @Test
    public void singleArrayDistinctArrayAggMultipleAssignments()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_AGG(a)), ARRAY_AGG(a) FROM (VALUES (1), (1)) t(a)"))
                .matches("VALUES (ARRAY [1], ARRAY [1, 1])");
    }

    @Test
    public void deeplyNestedReferenceToArrayAggWithOtherSymbols()
    {
        assertThat(assertions.query(
                "SELECT ARRAY_DISTINCT(ARRAY_AGG(a)), ARRAY_MIN(ARRAY_AGG(a)) + ARRAY_MAX(ARRAY_AGG(b)) FROM (VALUES (1, 2), (1, 2)) t(a, b)"))
                .matches("VALUES (ARRAY [1], 3)");
    }
}
