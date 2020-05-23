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
import com.google.common.collect.ImmutableListMultimap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.UnionNode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.prestosql.sql.planner.plan.Patterns.union;

/**
 * Transforms
 * <pre>
 * - Project (a)
 *      - Union
 *        output mappings: {a->c, a->e, b->d, b->f}
 *          - Source (c, d)
 *          - Source (e, f)
 * </pre>
 * into:
 * <pre>
 * - Project (a)
 *      - Union
 *        output mappings: {a->c, a->e}
 *          - Source (c, d)
 *          - Source (e, f)
 * </pre>
 * Note: as a result of this rule, the UnionNode's sources
 * are eligible for pruning outputs. This is accomplished
 * by PruneUnionSourceColumns rule.
 */
public class PruneUnionColumns
        extends ProjectOffPushDownRule<UnionNode>
{
    public PruneUnionColumns()
    {
        super(union());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, UnionNode unionNode, Set<Symbol> referencedOutputs)
    {
        ImmutableListMultimap<Symbol, Symbol> prunedOutputMappings = unionNode.getSymbolMapping().entries().stream()
                .filter(entry -> referencedOutputs.contains(entry.getKey()))
                .collect(toImmutableListMultimap(Map.Entry::getKey, Map.Entry::getValue));

        return Optional.of(
                new UnionNode(
                        unionNode.getId(),
                        unionNode.getSources(),
                        prunedOutputMappings,
                        ImmutableList.copyOf(prunedOutputMappings.keySet())));
    }
}
