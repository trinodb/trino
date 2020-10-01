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
package io.prestosql.tests;

import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.testing.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test {@link io.prestosql.sql.query.QueryAssertions}.
 */
public abstract class AbstractQueryAssertionsTest
        extends AbstractTestQueryFramework
{
    @Test
    public void testMatches()
    {
        assertThat(query("SELECT name FROM nation WHERE nationkey = 3"))
                .matches("VALUES CAST('CANADA' AS varchar(25))");
    }

    @Test
    public void testIsCorrectlyPushedDown()
    {
        assertThat(query("SELECT name FROM nation")).isCorrectlyPushedDown();

        // Test that, in case of failure, there is no failure when rendering expected and actual plans
        assertThatThrownBy(() -> assertThat(query("SELECT name FROM nation WHERE rand() = 42")).isCorrectlyPushedDown())
                .hasMessageContaining(
                        "Plan does not match, expected [\n" +
                                "\n" +
                                "- node(OutputNode)\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] but found [\n" +
                                "\n" +
                                "Output[name]\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] which resolves to [\n" +
                                "\n" +
                                "Output[name]");
    }

    @Test
    public void testIsNotFullyPushedDown()
    {
        assertThat(query("SELECT name FROM nation WHERE rand() = 42")).isNotFullyPushedDown(FilterNode.class);

        // Test that, in case of failure, there is no failure when rendering expected and actual plans
        assertThatThrownBy(() -> assertThat(query("SELECT name FROM nation")).isNotFullyPushedDown(FilterNode.class))
                .hasMessageContaining(
                        "Plan does not match, expected [\n" +
                                "\n" +
                                "- anyTree\n" +
                                "    - node(FilterNode)\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] but found [\n" +
                                "\n" +
                                "Output[name]\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] which resolves to [\n" +
                                "\n" +
                                "Output[name]");
    }
}
