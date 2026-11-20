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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class TestSetOperationNode
{
    @Test
    public void testRepeatedInputSymbols()
    {
        Symbol first = new Symbol(BIGINT, "first");
        Symbol second = new Symbol(BIGINT, "second");
        Symbol output = new Symbol(BIGINT, "output");
        Symbol repeatedOutput = new Symbol(BIGINT, "repeated_output");
        UnionNode union = new UnionNode(
                new PlanNodeId("union"),
                ImmutableList.of(
                        new ValuesNode(new PlanNodeId("first"), ImmutableList.of(new Symbol(BIGINT, "unused"), first)),
                        new ValuesNode(new PlanNodeId("second"), ImmutableList.of(second))),
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .putAll(output, new Symbol(BIGINT, "first"), new Symbol(BIGINT, "second"))
                        .putAll(repeatedOutput, first, second)
                        .build(),
                ImmutableList.of(output, repeatedOutput));

        assertThat(union.getOutputSymbols()).containsExactly(output, repeatedOutput);
        assertThat(union.sourceOutputLayout(0)).containsExactly(first, first);
        assertThat(union.sourceOutputLayout(1)).containsExactly(second, second);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    public void testInputMustComeFromCorrespondingSource(int invalidSource)
    {
        Symbol first = new Symbol(BIGINT, "first");
        Symbol second = new Symbol(BIGINT, "second");
        Symbol output = new Symbol(BIGINT, "output");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(
                                new ValuesNode(new PlanNodeId("first"), ImmutableList.of(first)),
                                new ValuesNode(new PlanNodeId("second"), ImmutableList.of(second))),
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .putAll(output, invalidSource == 0 ? second : first, invalidSource == 1 ? first : second)
                                .build(),
                        ImmutableList.of(output)))
                .withMessage("Source does not provide required symbols");
    }
}
