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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.DescriptorArgument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableArgument;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import io.trino.sql.tree.SymbolReference;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;

public class TableFunctionMatcher
        implements Matcher
{
    private final String name;
    private final Map<String, ArgumentValue> arguments;
    private final List<String> properOutputs;
    private final List<List<String>> copartitioningLists;

    private TableFunctionMatcher(
            String name,
            Map<String, ArgumentValue> arguments,
            List<String> properOutputs,
            List<List<String>> copartitioningLists)
    {
        this.name = requireNonNull(name, "name is null");
        this.arguments = ImmutableMap.copyOf(requireNonNull(arguments, "arguments is null"));
        this.properOutputs = ImmutableList.copyOf(requireNonNull(properOutputs, "properOutputs is null"));
        requireNonNull(copartitioningLists, "copartitioningLists is null");
        this.copartitioningLists = copartitioningLists.stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableFunctionNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableFunctionNode tableFunctionNode = (TableFunctionNode) node;

        if (!name.equals(tableFunctionNode.getName())) {
            return NO_MATCH;
        }

        if (arguments.size() != tableFunctionNode.getArguments().size()) {
            return NO_MATCH;
        }
        for (Map.Entry<String, ArgumentValue> entry : arguments.entrySet()) {
            String name = entry.getKey();
            Argument actual = tableFunctionNode.getArguments().get(name);
            if (actual == null) {
                return NO_MATCH;
            }
            ArgumentValue expected = entry.getValue();
            if (expected instanceof DescriptorArgumentValue expectedDescriptor) {
                if (!(actual instanceof DescriptorArgument actualDescriptor) || !expectedDescriptor.descriptor().equals(actualDescriptor.getDescriptor())) {
                    return NO_MATCH;
                }
            }
            else if (expected instanceof ScalarArgumentValue expectedScalar) {
                if (!(actual instanceof ScalarArgument actualScalar) || !Objects.equals(expectedScalar.value(), actualScalar.getValue())) {
                    return NO_MATCH;
                }
            }
            else {
                if (!(actual instanceof TableArgument)) {
                    return NO_MATCH;
                }
                TableArgumentValue expectedTableArgument = (TableArgumentValue) expected;
                TableArgumentProperties argumentProperties = tableFunctionNode.getTableArgumentProperties().get(expectedTableArgument.sourceIndex());
                if (!name.equals(argumentProperties.getArgumentName())) {
                    return NO_MATCH;
                }
                if (expectedTableArgument.rowSemantics() != argumentProperties.isRowSemantics() ||
                        expectedTableArgument.pruneWhenEmpty() != argumentProperties.isPruneWhenEmpty() ||
                        expectedTableArgument.passThroughColumns() != argumentProperties.getPassThroughSpecification().declaredAsPassThrough()) {
                    return NO_MATCH;
                }
                boolean specificationMatches = expectedTableArgument.specification()
                        .map(specification -> specification.getExpectedValue(symbolAliases))
                        .equals(argumentProperties.getSpecification());
                if (!specificationMatches) {
                    return NO_MATCH;
                }
                Set<SymbolReference> expectedPassThrough = expectedTableArgument.passThroughSymbols().stream()
                        .map(symbolAliases::get)
                        .collect(toImmutableSet());
                Set<SymbolReference> actualPassThrough = argumentProperties.getPassThroughSpecification().columns().stream()
                        .map(PassThroughColumn::symbol)
                        .map(Symbol::toSymbolReference)
                        .collect(toImmutableSet());
                if (!expectedPassThrough.equals(actualPassThrough)) {
                    return NO_MATCH;
                }
            }
        }

        if (properOutputs.size() != tableFunctionNode.getProperOutputs().size()) {
            return NO_MATCH;
        }

        if (!ImmutableSet.copyOf(copartitioningLists).equals(ImmutableSet.copyOf(tableFunctionNode.getCopartitioningLists()))) {
            return NO_MATCH;
        }

        ImmutableMap.Builder<String, SymbolReference> properOutputsMapping = ImmutableMap.builder();
        for (int i = 0; i < properOutputs.size(); i++) {
            properOutputsMapping.put(properOutputs.get(i), tableFunctionNode.getProperOutputs().get(i).toSymbolReference());
        }

        return match(SymbolAliases.builder()
                .putAll(symbolAliases)
                .putAll(properOutputsMapping.buildOrThrow())
                .build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("name", name)
                .add("arguments", arguments)
                .add("properOutputs", properOutputs)
                .add("copartitioningLists", copartitioningLists)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern[] sources;
        private String name;
        private final ImmutableMap.Builder<String, ArgumentValue> arguments = ImmutableMap.builder();
        private List<String> properOutputs = ImmutableList.of();
        private final ImmutableList.Builder<List<String>> copartitioningLists = ImmutableList.builder();

        Builder(PlanMatchPattern... sources)
        {
            this.sources = Arrays.copyOf(sources, sources.length);
        }

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder addDescriptorArgument(String name, DescriptorArgumentValue descriptor)
        {
            this.arguments.put(name, descriptor);
            return this;
        }

        public Builder addScalarArgument(String name, Object value)
        {
            this.arguments.put(name, new ScalarArgumentValue(value));
            return this;
        }

        public Builder addTableArgument(String name, TableArgumentValue.Builder tableArgument)
        {
            this.arguments.put(name, tableArgument.build());
            return this;
        }

        public Builder properOutputs(List<String> properOutputs)
        {
            this.properOutputs = properOutputs;
            return this;
        }

        public Builder addCopartitioning(List<String> copartitioning)
        {
            this.copartitioningLists.add(copartitioning);
            return this;
        }

        public PlanMatchPattern build()
        {
            return node(TableFunctionNode.class, sources)
                    .with(new TableFunctionMatcher(name, arguments.buildOrThrow(), properOutputs, copartitioningLists.build()));
        }
    }

    public sealed interface ArgumentValue
            permits DescriptorArgumentValue, ScalarArgumentValue, TableArgumentValue
    {}

    public record DescriptorArgumentValue(Optional<Descriptor> descriptor)
            implements ArgumentValue
    {
        public DescriptorArgumentValue(Optional<Descriptor> descriptor)
        {
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
        }

        public static DescriptorArgumentValue descriptorArgument(Descriptor descriptor)
        {
            return new DescriptorArgumentValue(Optional.of(requireNonNull(descriptor, "descriptor is null")));
        }

        public static DescriptorArgumentValue nullDescriptor()
        {
            return new DescriptorArgumentValue(Optional.empty());
        }
    }

    public record ScalarArgumentValue(Object value)
            implements ArgumentValue
    {}

    public record TableArgumentValue(
            int sourceIndex,
            boolean rowSemantics,
            boolean pruneWhenEmpty,
            boolean passThroughColumns,
            Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
            Set<String> passThroughSymbols)
            implements ArgumentValue
    {
        public TableArgumentValue(
                int sourceIndex,
                boolean rowSemantics,
                boolean pruneWhenEmpty,
                boolean passThroughColumns,
                Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
                Set<String> passThroughSymbols)
        {
            this.sourceIndex = sourceIndex;
            this.rowSemantics = rowSemantics;
            this.pruneWhenEmpty = pruneWhenEmpty;
            this.passThroughColumns = passThroughColumns;
            this.specification = requireNonNull(specification, "specification is null");
            this.passThroughSymbols = ImmutableSet.copyOf(passThroughSymbols);
        }

        public static class Builder
        {
            private final int sourceIndex;
            private boolean rowSemantics;
            private boolean pruneWhenEmpty;
            private boolean passThroughColumns;
            private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();
            private Set<String> passThroughSymbols = ImmutableSet.of();

            private Builder(int sourceIndex)
            {
                this.sourceIndex = sourceIndex;
            }

            public static Builder tableArgument(int sourceIndex)
            {
                return new Builder(sourceIndex);
            }

            public Builder rowSemantics()
            {
                this.rowSemantics = true;
                this.pruneWhenEmpty = true;
                return this;
            }

            public Builder pruneWhenEmpty()
            {
                this.pruneWhenEmpty = true;
                return this;
            }

            public Builder passThroughColumns()
            {
                this.passThroughColumns = true;
                return this;
            }

            public Builder specification(ExpectedValueProvider<DataOrganizationSpecification> specification)
            {
                this.specification = Optional.of(specification);
                return this;
            }

            public Builder passThroughSymbols(Set<String> symbols)
            {
                this.passThroughSymbols = symbols;
                return this;
            }

            private TableArgumentValue build()
            {
                return new TableArgumentValue(sourceIndex, rowSemantics, pruneWhenEmpty, passThroughColumns, specification, passThroughSymbols);
            }
        }
    }
}
