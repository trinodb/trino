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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.sql.planner.plan.Patterns.Values.rowCount;
import static io.trino.sql.planner.plan.Patterns.tableFinish;
import static io.trino.sql.planner.plan.Patterns.values;

/**
 * If the predicate for a table execute is optimized to false, the target table scan
 * of table execute will be replaced with an empty values node. This type of
 * plan cannot be executed and is meaningless anyway, so we replace the
 * entire thing with a values node.
 * <p>
 * Transforms
 * <pre>
 *  - TableFinish
 *    - (optional) Exchange
 *      - TableExecute
 *        - (optional) Exchange
 *          - empty Values
 * </pre>
 * into
 * <pre>
 *  - Values (null)
 * </pre>
 */
public class RemoveEmptyTableExecute
        implements Rule<TableFinishNode>
{
    private static final Pattern<TableFinishNode> PATTERN = tableFinish();

    private static Pattern<ValuesNode> emptyValues()
    {
        return values().with(rowCount().equalTo(0));
    }

    @Override
    public Pattern<TableFinishNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFinishNode finishNode, Captures captures, Context context)
    {
        Optional<PlanNode> finishSource = getSingleSourceSkipExchange(finishNode, context.getLookup());
        if (finishSource.isEmpty() || !(finishSource.get() instanceof TableExecuteNode)) {
            return Result.empty();
        }

        Optional<PlanNode> tableExecuteSource = getSingleSourceSkipExchange(finishSource.get(), context.getLookup());
        if (tableExecuteSource.isEmpty() || !(tableExecuteSource.get() instanceof ValuesNode valuesNode)) {
            return Result.empty();
        }
        verify(valuesNode.getRowCount() == 0, "Unexpected non-empty Values as source of TableExecuteNode");

        return Result.ofPlanNode(
                new ValuesNode(
                        finishNode.getId(),
                        finishNode.getOutputSymbols(),
                        ImmutableList.of(new Row(ImmutableList.of(new NullLiteral())))));
    }

    private Optional<PlanNode> getSingleSourceSkipExchange(PlanNode node, Lookup lookup)
    {
        if (node.getSources().size() != 1) {
            return Optional.empty();
        }
        PlanNode source = lookup.resolve(node.getSources().get(0));
        if (source instanceof ExchangeNode) {
            if (source.getSources().size() != 1) {
                return Optional.empty();
            }
            PlanNode exchangeSource = lookup.resolve(source.getSources().get(0));
            return Optional.of(lookup.resolve(exchangeSource));
        }
        return Optional.of(source);
    }
}
