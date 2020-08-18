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
import io.trino.spi.function.BoundSignature;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.plan.Patterns.window;

public class ReplaceWindowWithRowNumber
        implements Rule<WindowNode>
{
    private final Pattern<WindowNode> pattern;

    public ReplaceWindowWithRowNumber(Metadata metadata)
    {
        this.pattern = window()
                .matching(window -> {
                    if (window.getWindowFunctions().size() != 1) {
                        return false;
                    }
                    BoundSignature signature = getOnlyElement(window.getWindowFunctions().values()).getResolvedFunction().getSignature();
                    return signature.getArgumentTypes().isEmpty() && signature.getName().equals("row_number");
                })
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
