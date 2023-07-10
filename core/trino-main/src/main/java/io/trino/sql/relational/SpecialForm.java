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
package io.trino.sql.relational;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.OperatorNameUtil;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.function.OperatorType.CAST;
import static java.util.Objects.requireNonNull;

public class SpecialForm
        extends RowExpression
{
    private final Form form;
    private final Type returnType;
    private final List<RowExpression> arguments;
    private final List<ResolvedFunction> functionDependencies;

    public SpecialForm(Form form, Type returnType, RowExpression... arguments)
    {
        this(form, returnType, ImmutableList.copyOf(arguments));
    }

    public SpecialForm(Form form, Type returnType, List<RowExpression> arguments)
    {
        this(form, returnType, arguments, ImmutableList.of());
    }

    public SpecialForm(Form form, Type returnType, List<RowExpression> arguments, List<ResolvedFunction> functionDependencies)
    {
        this.form = requireNonNull(form, "form is null");
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.functionDependencies = ImmutableList.copyOf(requireNonNull(functionDependencies, "functionDependencies is null"));
    }

    public Form getForm()
    {
        return form;
    }

    public List<ResolvedFunction> getFunctionDependencies()
    {
        return functionDependencies;
    }

    public ResolvedFunction getOperatorDependency(OperatorType operator)
    {
        String mangleOperatorName = OperatorNameUtil.mangleOperatorName(operator);
        for (ResolvedFunction function : functionDependencies) {
            if (function.getSignature().getName().equals(mangleOperatorName)) {
                return function;
            }
        }
        throw new IllegalArgumentException("Expected operator: " + operator);
    }

    public Optional<ResolvedFunction> getCastDependency(Type fromType, Type toType)
    {
        if (fromType.equals(toType)) {
            return Optional.empty();
        }
        BoundSignature boundSignature = new BoundSignature(OperatorNameUtil.mangleOperatorName(CAST), toType, ImmutableList.of(fromType));
        for (ResolvedFunction function : functionDependencies) {
            if (function.getSignature().equals(boundSignature)) {
                return Optional.of(function);
            }
        }
        throw new IllegalArgumentException("Expected cast: " + boundSignature);
    }

    @Override
    public Type getType()
    {
        return returnType;
    }

    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return form.name() + "(" + Joiner.on(", ").join(arguments) + ")";
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
        SpecialForm that = (SpecialForm) o;
        return form == that.form &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(functionDependencies, that.functionDependencies);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(form, returnType, arguments, functionDependencies);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitSpecialForm(this, context);
    }

    public enum Form
    {
        IF,
        NULL_IF,
        SWITCH,
        WHEN,
        BETWEEN,
        IS_NULL,
        COALESCE,
        IN,
        AND,
        OR,
        DEREFERENCE,
        ROW_CONSTRUCTOR,
        BIND,
    }
}
