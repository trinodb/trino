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
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.plan.Patterns.window;

public class ReplaceWindowWithRowNumber
        implements Rule<WindowNode>
{
    private final Pattern<WindowNode> pattern;

    public ReplaceWindowWithRowNumber(Metadata metadata)
    {
        this.pattern = window()
                .matching(window -> window.getWindowFunctions().size() == 1 && getOnlyElement(window.getWindowFunctions().values()).getResolvedFunction().getFunctionId().equals(metadata.resolveFunction(QualifiedName.of("row_number"), ImmutableList.of()).getFunctionId()))
                .matching(window -> window.getOrderingScheme().isEmpty());
    }

    @Override
    public Pattern<WindowNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(WindowNode node, Captures captures, Context context)
    {
        return Result.ofPlanNode(new RowNumberNode(
                node.getId(),
                node.getSource(),
                node.getPartitionBy(),
                false,
                getOnlyElement(node.getWindowFunctions().keySet()),
                Optional.empty(),
                Optional.empty()));
    }
}
