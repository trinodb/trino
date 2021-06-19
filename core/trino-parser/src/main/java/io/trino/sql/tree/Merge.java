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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Merge
        extends Statement
{
    private final Table table;
    private final Optional<Identifier> targetAlias;
    private final Relation relation;
    private final Expression expression;
    private final List<MergeCase> mergeCases;

    public Merge(
            Table table,
            Optional<Identifier> targetAlias,
            Relation relation,
            Expression expression,
            List<MergeCase> mergeCases)

    {
        this(Optional.empty(), table, targetAlias, relation, expression, mergeCases);
    }

    public Merge(
            NodeLocation location,
            Table table,
            Optional<Identifier> targetAlias,
            Relation relation,
            Expression expression,
            List<MergeCase> mergeCases)
    {
        this(Optional.of(location), table, targetAlias, relation, expression, mergeCases);
    }

    public Merge(
            Optional<NodeLocation> location,
            Table table,
            Optional<Identifier> targetAlias,
            Relation relation,
            Expression expression,
            List<MergeCase> mergeCases)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.targetAlias = requireNonNull(targetAlias, "targetAlias is null");
        this.relation = requireNonNull(relation, "relation is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.mergeCases = ImmutableList.copyOf(requireNonNull(mergeCases, "mergeCases is null"));
    }

    public Table getTable()
    {
        return table;
    }

    public Optional<Identifier> getTargetAlias()
    {
        return targetAlias;
    }

    public Relation getRelation()
    {
        return relation;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public List<MergeCase> getMergeCases()
    {
        return mergeCases;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMerge(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        builder.add(table);
        builder.add(relation);
        builder.add(expression);
        builder.addAll(mergeCases);
        return builder.build();
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
        Merge merge = (Merge) o;
        return Objects.equals(table, merge.table) &&
                Objects.equals(targetAlias, merge.targetAlias) &&
                Objects.equals(relation, merge.relation) &&
                Objects.equals(expression, merge.expression) &&
                Objects.equals(mergeCases, merge.mergeCases);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, targetAlias, relation, expression, mergeCases);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("targetAlias", targetAlias.orElse(null))
                .add("relation", relation)
                .add("expression", expression)
                .add("mergeCases", mergeCases)
                .omitNullValues()
                .toString();
    }
}
