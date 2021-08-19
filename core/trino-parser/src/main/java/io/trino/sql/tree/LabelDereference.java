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

import static java.util.Objects.requireNonNull;

/**
 * A temporary IR representation of a label-prefixed column reference
 * in the context of row pattern recognition.
 * <p>
 * It is created from a DereferenceExpression when the MEASURES or DEFINE
 * expressions are rewritten using the TranslationMap:
 * A.price -> DereferenceExpression("A", "price") -> LabelDereference("A", price_symbol).
 * Next, the LabelDereference is processed by the LogicalIndexExtractor,
 * and it is removed from the expression.
 * <p>
 * LabelDereference is a synthetic AST node. It had to be introduced in order to carry
 * the rewritten symbol (`price_symbol` in the example). The DereferenceExpression
 * cannot be used for that purpose, because it only contains identifiers, and a Symbol
 * cannot be safely converted to an Identifier.
 */
public class LabelDereference
        extends Expression
{
    private final String label;
    private final SymbolReference reference;

    public LabelDereference(String label, SymbolReference reference)
    {
        super(Optional.empty());
        this.label = requireNonNull(label, "label is null");
        this.reference = requireNonNull(reference, "reference is null");
    }

    public String getLabel()
    {
        return label;
    }

    public SymbolReference getReference()
    {
        return reference;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLabelDereference(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(reference);
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
        LabelDereference that = (LabelDereference) o;
        return Objects.equals(label, that.label) &&
                Objects.equals(reference, that.reference);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, reference);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return label.equals(((LabelDereference) other).label);
    }
}
