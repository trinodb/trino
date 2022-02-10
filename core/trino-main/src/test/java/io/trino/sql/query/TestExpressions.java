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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestExpressions
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
    public void testBooleanExpressionInCase()
    {
        assertThat(assertions.query("VALUES CASE 1 IS NULL WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 IS NOT NULL WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 BETWEEN 0 AND 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 NOT BETWEEN 0 AND 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 IN (1, 2) WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 NOT IN (1, 2) WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 = 1 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 = 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
        assertThat(assertions.query("VALUES CASE 1 < 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 10");
        assertThat(assertions.query("VALUES CASE 1 > 2 WHEN true THEN 10 ELSE 20 END")).matches("VALUES 20");
    }

    @Test
    public void testInShortCircuit()
    {
        // Because of the failing in-list item 5 / 0, the in-predicate cannot be simplified.
        // Also, because of the non-deterministic expression `rand()` referenced twice in
        // the in-predicate (`x + x`), the in-predicate cannot be inlined into Values.
        // It is instead processed with the use of generated code which applies the short-circuit
        // logic and finds a match without evaluating the failing item.
        // According to in-predicate semantics, this query should fail, as all the in-list items
        // should be successfully evaluated before a check for the match is performed.
        // However, the execution of in-predicate is optimized for efficiency at the cost of
        // correctness in this edge case.
        assertThat(assertions.query("SELECT IF(3 IN (2, 4, 3, 5 / 0), 1e0, x + x) FROM (VALUES rand()) t(x)")).matches("VALUES 1e0");

        // the in-predicate is inlined into Values and evaluated by the ExpressionInterpreter: eager evaluation, failure.
        assertThatThrownBy(() -> assertions.query("SELECT 3 IN (2, 4, 3, 5 / 0)"))
                .hasMessage("Division by zero");
    }

    @Test
    public void testInlineNullBind()
    {
        // https://github.com/trinodb/trino/issues/3411
        assertThat(assertions.query("SELECT try(k) FROM (SELECT null) t(k)")).matches("VALUES null");
    }
}
