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
package io.trino.operator.window.matcher;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.PagesIndex;
import io.trino.operator.window.pattern.LabelEvaluator;
import io.trino.operator.window.pattern.MatchAggregation;
import io.trino.operator.window.pattern.PhysicalValueAccessor;
import io.trino.operator.window.pattern.ProjectingPagesWindowIndex;
import io.trino.spi.Page;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.util.CanIgnoreReturnValue;

import java.util.List;
import java.util.Map;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.assertj.core.api.Assertions.assertThat;

public class MatchAssert
        extends AbstractAssert<MatchAssert, MatchResult>
{
    private final Map<IrLabel, Integer> labelMapping;

    private MatchAssert(MatchResult actual, Map<IrLabel, Integer> labelMapping)
    {
        super(actual, Object.class);
        this.labelMapping = labelMapping;
    }

    public static AssertProvider<MatchAssert> match(IrRowPattern pattern, String input, Map<IrLabel, Integer> labelMapping)
    {
        Program program = IrRowPatternToProgramRewriter.rewrite(pattern, labelMapping);
        List<List<PhysicalValueAccessor>> dummyPointers = ImmutableList.of(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
        Matcher matcher = new Matcher(program, dummyPointers, ImmutableList.of(), ImmutableList.of());

        int[] mappedInput = new int[input.length()];
        char[] chars = input.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            mappedInput[i] = labelMapping.get(new IrLabel(String.valueOf(chars[i])));
        }

        return () -> new MatchAssert(matcher.run(identityEvaluator(mappedInput), new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "dummy"), newSimpleAggregatedMemoryContext()), labelMapping);
    }

    @CanIgnoreReturnValue
    public MatchAssert hasLabels(char[] expectedLabels)
    {
        int[] mappedExpected = new int[expectedLabels.length];
        for (int i = 0; i < expectedLabels.length; i++) {
            mappedExpected[i] = labelMapping.get(new IrLabel(String.valueOf(expectedLabels[i])));
        }
        return satisfies(actual -> assertThat(actual.isMatched()).isTrue())
                .satisfies(actual -> assertThat(actual.getLabels().toArray())
                        .as("Matched labels")
                        .isEqualTo(mappedExpected));
    }

    @CanIgnoreReturnValue
    public MatchAssert hasCaptures(int[] expectedCaptures)
    {
        return satisfies(actual -> assertThat(actual.isMatched()).isTrue())
                .satisfies(actual -> assertThat(actual.getExclusions().toArray())
                        .as("Captured exclusions")
                        .isEqualTo(expectedCaptures));
    }

    @CanIgnoreReturnValue
    public MatchAssert isNoMatch()
    {
        return satisfies(actual -> assertThat(actual.isMatched()).isFalse());
    }

    private static LabelEvaluator identityEvaluator(int[] input)
    {
        // create dummy WindowIndex for the LabelEvaluator
        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false).newPagesIndex(ImmutableList.of(), 1);
        pagesIndex.addPage(new Page(1));
        return new IdentityEvaluator(input, new ProjectingPagesWindowIndex(pagesIndex, 0, 1, ImmutableList.of(), ImmutableList.of()));
    }

    private static class IdentityEvaluator
            extends LabelEvaluator
    {
        private final int[] input;

        public IdentityEvaluator(int[] input, ProjectingPagesWindowIndex dummy)
        {
            super(0, 0, 0, 0, 1, ImmutableList.of(), dummy);
            this.input = input;
        }

        @Override
        public int getInputLength()
        {
            return input.length;
        }

        @Override
        public boolean isMatchingAtPartitionStart()
        {
            return true;
        }

        @Override
        public boolean evaluateLabel(ArrayView matchedLabels, MatchAggregation[] aggregations)
        {
            int position = matchedLabels.length() - 1;
            return input[position] == matchedLabels.get(position);
        }
    }
}
