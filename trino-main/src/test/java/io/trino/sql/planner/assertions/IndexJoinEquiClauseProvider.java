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
package io.trino.sql.planner.assertions;

import io.trino.sql.planner.plan.IndexJoinNode;

import static java.util.Objects.requireNonNull;

class IndexJoinEquiClauseProvider
        implements ExpectedValueProvider<IndexJoinNode.EquiJoinClause>
{
    private final SymbolAlias probe;
    private final SymbolAlias index;

    IndexJoinEquiClauseProvider(SymbolAlias probe, SymbolAlias index)
    {
        this.probe = requireNonNull(probe, "probe is null");
        this.index = requireNonNull(index, "index is null");
    }

    @Override
    public IndexJoinNode.EquiJoinClause getExpectedValue(SymbolAliases aliases)
    {
        return new IndexJoinNode.EquiJoinClause(probe.toSymbol(aliases), index.toSymbol(aliases));
    }

    @Override
    public String toString()
    {
        return probe + " = " + index;
    }
}
