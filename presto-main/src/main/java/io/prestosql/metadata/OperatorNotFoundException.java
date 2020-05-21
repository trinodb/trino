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
package io.prestosql.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.OPERATOR_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OperatorNotFoundException
        extends PrestoException
{
    private final OperatorType operatorType;
    private final TypeSignature returnType;
    private final List<Type> argumentTypes;

    public OperatorNotFoundException(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        super(OPERATOR_NOT_FOUND, formatErrorMessage(operatorType, argumentTypes, Optional.empty()));
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.returnType = null;
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    public OperatorNotFoundException(OperatorType operatorType, List<? extends Type> argumentTypes, TypeSignature returnType)
    {
        super(OPERATOR_NOT_FOUND, formatErrorMessage(operatorType, argumentTypes, Optional.of(returnType)));
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.returnType = requireNonNull(returnType, "returnType is null");
    }

    private static String formatErrorMessage(OperatorType operatorType, List<? extends Type> argumentTypes, Optional<TypeSignature> returnType)
    {
        switch (operatorType) {
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULUS:
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return format("Cannot apply operator: %s %s %s", argumentTypes.get(0), operatorType.getOperator(), argumentTypes.get(1));
            case NEGATION:
                return format("Cannot negate %s", argumentTypes.get(0));
            case IS_DISTINCT_FROM:
                return format("Cannot check if %s is distinct from %s", argumentTypes.get(0), argumentTypes.get(1));
            case CAST:
                return format("Cannot cast %s to %s", argumentTypes.get(0), returnType.orElseThrow());
            case SUBSCRIPT:
                return format("Cannot use %s for subscript of %s", argumentTypes.get(1), argumentTypes.get(0));
        }
        return format(
                "Operator '%s'%s cannot be applied to %s",
                operatorType.getOperator(),
                returnType.map(value -> ":" + value).orElse(""),
                Joiner.on(", ").join(argumentTypes));
    }

    public OperatorType getOperatorType()
    {
        return operatorType;
    }

    public TypeSignature getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }
}
