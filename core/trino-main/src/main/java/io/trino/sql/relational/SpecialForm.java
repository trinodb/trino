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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.spi.function.OperatorType.CAST;
import static java.util.Objects.requireNonNull;

public record SpecialForm(
        io.trino.sql.relational.SpecialForm.Form form,
        Type type,
        List<RowExpression> arguments,
        List<ResolvedFunction> functionDependencies)
        implements RowExpression
{
    public SpecialForm
    {
        requireNonNull(form, "form is null");
        requireNonNull(type, "type is null");
        requireNonNull(arguments, "arguments is null");
        functionDependencies = ImmutableList.copyOf(requireNonNull(functionDependencies, "functionDependencies is null"));
    }

    public ResolvedFunction getOperatorDependency(OperatorType operator)
    {
        String mangleOperatorName = mangleOperatorName(operator);
        for (ResolvedFunction function : functionDependencies) {
            if (function.signature().getName().getFunctionName().equalsIgnoreCase(mangleOperatorName)) {
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
        BoundSignature boundSignature = new BoundSignature(builtinFunctionName(CAST), toType, ImmutableList.of(fromType));
        for (ResolvedFunction function : functionDependencies) {
            if (function.signature().equals(boundSignature)) {
                return Optional.of(function);
            }
        }
        throw new IllegalArgumentException("Expected cast: " + boundSignature);
    }

    @Override
    public String toString()
    {
        return form.name() + "(" + Joiner.on(", ").join(arguments) + ")";
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
        ARRAY_CONSTRUCTOR,
        BIND,
    }
}
