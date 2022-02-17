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
package io.trino.plugin.jdbc.expression;

import io.trino.sql.tree.ComparisonExpression;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRewriteComparison
{
    @Test
    public void testOperatorEnumsInSync()
    {
        assertThat(
                Stream.of(RewriteComparison.ComparisonOperator.values())
                        .map(Enum::name))
                .containsExactlyInAnyOrder(
                        Stream.of(ComparisonExpression.Operator.values())
                                .map(Enum::name)
                                .toArray(String[]::new));
    }
}
