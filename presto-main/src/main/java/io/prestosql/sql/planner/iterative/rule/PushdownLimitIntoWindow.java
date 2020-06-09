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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.FunctionId;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.isOptimizeTopNRowNumber;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.window;
import static java.lang.Math.toIntExact;

public class PushdownLimitIntoWindow
        implements Rule<LimitNode>
{
    private final Capture<WindowNode> childCapture;
    private final Pattern<LimitNode> pattern;

    private final FunctionId rowNumberFunctionId;

    public PushdownLimitIntoWindow(Metadata metadata)
    {
        this.rowNumberFunctionId = metadata.resolveFunction(QualifiedName.of("row_number"), ImmutableList.of()).getFunctionId();
        this.childCapture = newCapture();
        this.pattern = limit()
                .matching(limit -> !limit.isWithTies() && limit.getCount() != 0 && limit.getCount() <= Integer.MAX_VALUE)
                .with(source().matching(window()
                        .matching(window -> window.getOrderingScheme().isPresent())
                        .matching(window -> window.getWindowFunctions().size() == 1 && getOnlyElement(window.getWindowFunctions().values()).getResolvedFunction().getFunctionId().equals(rowNumberFunctionId))
                        .capturedAs(childCapture)));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeTopNRowNumber(session);
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(LimitNode node, Captures captures, Context context)
    {
        WindowNode source = captures.get(childCapture);
        int limit = toIntExact(node.getCount());
        TopNRowNumberNode topNRowNumberNode = new TopNRowNumberNode(
                source.getId(),
                source.getSource(),
                source.getSpecification(),
                getOnlyElement(source.getWindowFunctions().keySet()),
                limit,
                false,
                Optional.empty());
        if (source.getPartitionBy().isEmpty()) {
            return Result.ofPlanNode(topNRowNumberNode);
        }
        return Result.ofPlanNode(replaceChildren(node, ImmutableList.of(topNRowNumberNode)));
    }
}
