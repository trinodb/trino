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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.TableFunctionHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.testing.TestingTransactionHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;

public class TableFunctionProcessorBuilder
{
    private String name;
    private List<Symbol> properOutputs = ImmutableList.of();
    private Optional<PlanNode> source = Optional.empty();
    private boolean pruneWhenEmpty;
    private List<PassThroughSpecification> passThroughSpecifications = ImmutableList.of();
    private List<List<Symbol>> requiredSymbols = ImmutableList.of();
    private Optional<Map<Symbol, Symbol>> markerSymbols = Optional.empty();
    private Optional<DataOrganizationSpecification> specification = Optional.empty();
    private Set<Symbol> prePartitioned = ImmutableSet.of();
    private int preSorted;
    private Optional<Symbol> hashSymbol = Optional.empty();

    public TableFunctionProcessorBuilder() {}

    public TableFunctionProcessorBuilder name(String name)
    {
        this.name = name;
        return this;
    }

    public TableFunctionProcessorBuilder properOutputs(Symbol... properOutputs)
    {
        this.properOutputs = ImmutableList.copyOf(properOutputs);
        return this;
    }

    public TableFunctionProcessorBuilder source(PlanNode source)
    {
        this.source = Optional.of(source);
        return this;
    }

    public TableFunctionProcessorBuilder pruneWhenEmpty()
    {
        this.pruneWhenEmpty = true;
        return this;
    }

    public TableFunctionProcessorBuilder passThroughSpecifications(PassThroughSpecification... passThroughSpecifications)
    {
        this.passThroughSpecifications = ImmutableList.copyOf(passThroughSpecifications);
        return this;
    }

    public TableFunctionProcessorBuilder requiredSymbols(List<List<Symbol>> requiredSymbols)
    {
        this.requiredSymbols = requiredSymbols;
        return this;
    }

    public TableFunctionProcessorBuilder markerSymbols(Map<Symbol, Symbol> markerSymbols)
    {
        this.markerSymbols = Optional.of(markerSymbols);
        return this;
    }

    public TableFunctionProcessorBuilder specification(DataOrganizationSpecification specification)
    {
        this.specification = Optional.of(specification);
        return this;
    }

    public TableFunctionProcessorBuilder prePartitioned(Set<Symbol> prePartitioned)
    {
        this.prePartitioned = prePartitioned;
        return this;
    }

    public TableFunctionProcessorBuilder preSorted(int preSorted)
    {
        this.preSorted = preSorted;
        return this;
    }

    public TableFunctionProcessorBuilder hashSymbol(Symbol hashSymbol)
    {
        this.hashSymbol = Optional.of(hashSymbol);
        return this;
    }

    public TableFunctionProcessorNode build(PlanNodeIdAllocator idAllocator)
    {
        return new TableFunctionProcessorNode(
                idAllocator.getNextId(),
                name,
                properOutputs,
                source,
                pruneWhenEmpty,
                passThroughSpecifications,
                requiredSymbols,
                markerSymbols,
                specification,
                prePartitioned,
                preSorted,
                hashSymbol,
                new TableFunctionHandle(TEST_CATALOG_HANDLE, new ConnectorTableFunctionHandle() {}, TestingTransactionHandle.create()));
    }
}
