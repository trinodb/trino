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

import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableWriterNode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;

public class TableWriterMatcher
        implements Matcher
{
    private final List<String> columns;
    private final List<String> columnNames;

    public TableWriterMatcher(List<String> columns, List<String> columnNames)
    {
        this.columns = columns;
        this.columnNames = columnNames;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableWriterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableWriterNode tableWriterNode = (TableWriterNode) node;
        if (!tableWriterNode.getColumnNames().equals(columnNames)) {
            return NO_MATCH;
        }

        if (!columns.stream()
                .map(s -> Symbol.from(symbolAliases.get(s)))
                .collect(toImmutableList())
                .equals(tableWriterNode.getColumns())) {
            return NO_MATCH;
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .add("columnNames", columnNames)
                .toString();
    }
}
