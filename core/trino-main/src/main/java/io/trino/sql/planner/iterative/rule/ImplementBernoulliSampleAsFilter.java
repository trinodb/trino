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
package io.trino.sql.planner.iterative.rule;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.QualifiedName;

import static io.trino.sql.planner.plan.Patterns.Sample.sampleType;
import static io.trino.sql.planner.plan.Patterns.sample;
import static io.trino.sql.planner.plan.SampleNode.Type.BERNOULLI;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 * - Sample(BERNOULLI, p)
 *     - X
 * </pre>
 * Into:
 * <pre>
 * - Filter (rand() < p)
 *     - X
 * </pre>
 */
public class ImplementBernoulliSampleAsFilter
        implements Rule<SampleNode>
{
    private static final Pattern<SampleNode> PATTERN = sample()
            .with(sampleType().equalTo(BERNOULLI));
    private final Metadata metadata;

    public ImplementBernoulliSampleAsFilter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<SampleNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SampleNode sample, Captures captures, Context context)
    {
        return Result.ofPlanNode(new FilterNode(
                sample.getId(),
                sample.getSource(),
                new ComparisonExpression(
                        ComparisonExpression.Operator.LESS_THAN,
                        FunctionCallBuilder.resolve(context.getSession(), metadata)
                                .setName(QualifiedName.of("rand"))
                                .build(),
                        new DoubleLiteral(Double.toString(sample.getSampleRatio())))));
    }
}
