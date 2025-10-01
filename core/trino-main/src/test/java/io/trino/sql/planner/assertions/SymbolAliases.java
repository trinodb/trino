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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.Assignments;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SymbolAliases
{
    private final Map<String, Reference> map;

    public SymbolAliases()
    {
        this.map = ImmutableMap.of();
    }

    private SymbolAliases(Map<String, Reference> aliases)
    {
        this.map = ImmutableMap.copyOf(requireNonNull(aliases, "aliases is null"));
    }

    public Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>() {
            @Override
            public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return map.getOrDefault(node.name(), node);
            }
        }, expression);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public SymbolAliases withNewAliases(SymbolAliases sourceAliases)
    {
        Builder builder = new Builder(this);

        for (Map.Entry<String, Reference> alias : sourceAliases.map.entrySet()) {
            builder.put(alias.getKey(), alias.getValue());
        }

        return builder.build();
    }

    public Symbol getSymbol(String alias)
    {
        return Symbol.from(get(alias));
    }

    public Reference get(String alias)
    {
        /*
         * It's still kind of an open question if the right combination of anyTree() and
         * a sufficiently complex and/or ambiguous plan might make throwing here a
         * theoretically incorrect thing to do.
         *
         * If you run into a case that you think justifies changing this, please consider
         * that it's already pretty hard to determine if a failure is because the test
         * is written incorrectly or because the actual plan really doesn't match a
         * correctly written test. Having this throw makes it a lot easier to track down
         * missing aliases in incorrect plans.
         */
        return getOptional(alias).orElseThrow(() -> new IllegalStateException(format("missing expression for alias %s", alias)));
    }

    public Optional<Reference> getOptional(String alias)
    {
        Reference result = map.get(alias);
        return Optional.ofNullable(result);
    }

    private Map<String, Reference> getUpdatedAssignments(Assignments assignments)
    {
        ImmutableMap.Builder<String, Reference> mapUpdate = ImmutableMap.builder();
        for (Map.Entry<Symbol, Expression> assignment : assignments.assignments().entrySet()) {
            for (Map.Entry<String, Reference> existingAlias : map.entrySet()) {
                if (assignment.getValue().equals(existingAlias.getValue())) {
                    // Simple symbol rename
                    mapUpdate.put(existingAlias.getKey(), assignment.getKey().toSymbolReference());
                }
                else if (assignment.getKey().toSymbolReference().equals(existingAlias.getValue())) {
                    /*
                     * Special case for nodes that can alias symbols in the node's assignment map.
                     * In this case, we've already added the alias in the map, but we won't include it
                     * as a simple rename as covered above. Add the existing alias to the result if
                     * the LHS of the assignment matches the symbol reference of the existing alias.
                     *
                     * This comes up when we alias expressions in project nodes for use further up the tree.
                     * At the beginning for the function, map contains { NEW_ALIAS: SymbolReference("expr_2" }
                     * and the assignments map contains { expr_2 := <some expression> }.
                     */
                    mapUpdate.put(existingAlias.getKey(), existingAlias.getValue());
                }
            }
        }
        return mapUpdate.buildOrThrow();
    }

    /*
     * Return a new SymbolAliases that contains a map with the original bindings
     * updated based on assignments given that assignments is a map of
     * newSymbol := oldSymbolReference.
     *
     * INCLUDE aliases for SymbolReferences that aren't in assignments.values()
     *
     * Example:
     * SymbolAliases = { "ALIAS1": SymbolReference("foo"), "ALIAS2": SymbolReference("bar")}
     * updateAssignments({"baz": SymbolReference("foo")})
     * returns a new
     * SymbolAliases = { "ALIAS1": SymbolReference("baz"), "ALIAS2": SymbolReference("bar")}
     */
    public SymbolAliases updateAssignments(Assignments assignments)
    {
        return builder()
                .putAll(this)
                .putUnchecked(getUpdatedAssignments(assignments))
                .build();
    }

    /*
     * Return a new SymbolAliases that contains a map with the original bindings
     * updated based on assignments given that assignments is a map of
     * newSymbol := oldSymbolReference.
     *
     * DISCARD aliases for SymbolReferences that aren't in assignments.values()
     *
     * Example:
     * SymbolAliases = { "ALIAS1": SymbolReference("foo"), "ALIAS2": SymbolReference("bar")}
     * updateAssignments({"baz": SymbolReference("foo")})
     * returns a new
     * SymbolAliases = { "ALIAS1": SymbolReference("baz") }
     *
     * When you pass through a project node, all of the aliases need to be updated, and
     * aliases for symbols that aren't projected need to be removed.
     *
     * Example in the context of a Plan:
     * PlanMatchPattern.tableScan("nation", ImmutableMap.of("NK", "nationkey", "RK", "regionkey")
     * applied to
     * TableScanNode { col1 := ColumnHandle(nation, nationkey), col2 := ColumnHandle(nation, regionkey) }
     * gives SymbolAliases.map
     * { "NK": SymbolReference("col1"), "RK": SymbolReference("col2") }
     *
     * ... Visit some other nodes, one of which presumably consumes col1, and none of which add any new aliases ...
     *
     * If we then visit a project node
     * Project { value3 := col2 }
     * SymbolAliases.map should be
     * { "RK": SymbolReference("value3") }
     */
    public SymbolAliases replaceAssignments(Assignments assignments)
    {
        return new SymbolAliases(getUpdatedAssignments(assignments));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(map)
                .toString();
    }

    public static class Builder
    {
        Map<String, Reference> bindings;

        private Builder()
        {
            bindings = new HashMap<>();
        }

        private Builder(SymbolAliases initialAliases)
        {
            bindings = new HashMap<>(initialAliases.map);
        }

        public Builder put(String alias, Reference reference)
        {
            requireNonNull(alias, "alias is null");
            requireNonNull(reference, "symbolReference is null");

            // Special case to allow identity binding (i.e. "ALIAS" -> expression("ALIAS"))
            if (bindings.containsKey(alias) && bindings.get(alias).equals(reference)) {
                return this;
            }

            checkState(!bindings.containsKey(alias), "Alias '%s' already bound to expression '%s'. Tried to rebind to '%s'", alias, bindings.get(alias), reference);
            bindings.put(alias, reference);
            return this;
        }

        public Builder putAll(Map<String, Reference> aliases)
        {
            aliases.entrySet()
                    .forEach(entry -> put(entry.getKey(), entry.getValue()));
            return this;
        }

        /*
         * This is supplied specifically for updateAssigments, which needs to
         * update existing bindings that have already been added. Unless you're
         * certain you want this behavior, you don't want it.
         */
        private Builder putUnchecked(Map<String, Reference> aliases)
        {
            bindings.putAll(aliases);
            return this;
        }

        public Builder putAll(SymbolAliases aliases)
        {
            return putAll(aliases.map);
        }

        public SymbolAliases build()
        {
            return new SymbolAliases(bindings);
        }
    }
}
