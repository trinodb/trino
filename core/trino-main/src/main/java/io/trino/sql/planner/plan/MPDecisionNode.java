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
package io.trino.sql.planner.plan;

import io.trino.metadata.TableHandle;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MPDecisionNode
        extends PlanNode
{
    // Should contain a single actual table, should collect all the TableHandles and pass to the connector.
    // We'll choose an option based on the preferred tableHandle
    Map<TableHandle, PlanNode> options; // Edit: I think it doesn't have to be SortedMap at this point

    public MPDecisionNode(PlanNodeId id, Map<TableHandle, PlanNode> options)
    {
        super(id);
        this.options = options;
    }

    @Override
    public List<PlanNode> getSources()
    {
        // TODO: It's actually only one of those. Is that ok to declare all of them as sources?
        return List.copyOf(options.values());
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return null;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return null;
    }

    public Map<TableHandle, PlanNode> getOptions()
    {
        return options;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MPDecisionNode mpFilter = (MPDecisionNode) o;
        return Objects.equals(options, mpFilter.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(options);
    }

    @Override
    public String toString()
    {
        return "MPDecisionNode{" +
                "options=" + options +
                '}';
    }
}
