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
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class ColumnHandleMatcher
        implements RvalueMatcher
{
    private final Predicate<ColumnHandle> matcher;

    public ColumnHandleMatcher(Predicate<ColumnHandle> handleMatcher)
    {
        this.matcher = requireNonNull(handleMatcher, "handleMatcher is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof TableScanNode)) {
            return Optional.empty();
        }

        Map<Symbol, ColumnHandle> assignments = ((TableScanNode) node).getAssignments();

        for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
            if (matcher.test(entry.getValue())) {
                return Optional.of(entry.getKey());
            }
        }

        return Optional.empty();
    }
}
