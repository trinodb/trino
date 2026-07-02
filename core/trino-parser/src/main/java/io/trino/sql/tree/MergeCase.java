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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class MergeCase
        extends Node
{
    protected final MergeCaseKind mergeCaseKind;
    protected final Optional<Expression> expression;

    protected MergeCase(NodeLocation location, MergeCaseKind mergeCaseKind, Optional<Expression> expression)
    {
        super(location);
        this.mergeCaseKind = requireNonNull(mergeCaseKind, "mergeCaseKind is null");
        this.expression = requireNonNull(expression, "expression is null");
    }

    @Deprecated
    protected MergeCase(Optional<NodeLocation> location, MergeCaseKind mergeCaseKind, Optional<Expression> expression)
    {
        super(location);
        this.mergeCaseKind = requireNonNull(mergeCaseKind, "mergeCaseKind is null");
        this.expression = requireNonNull(expression, "expression is null");
    }

    public MergeCaseKind getMergeCaseKind()
    {
        return mergeCaseKind;
    }

    public Optional<Expression> getExpression()
    {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMergeCase(this, context);
    }

    public abstract List<Identifier> getSetColumns();

    public abstract List<Expression> getSetExpressions();
}
