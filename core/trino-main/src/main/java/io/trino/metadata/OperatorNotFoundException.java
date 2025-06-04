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
package io.trino.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.OPERATOR_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OperatorNotFoundException
        extends TrinoException
{
    private final OperatorType operatorType;
    private final TypeSignature returnType;
    private final List<Type> argumentTypes;

    public OperatorNotFoundException(OperatorType operatorType, List<? extends Type> argumentTypes, Throwable cause)
    {
        super(OPERATOR_NOT_FOUND, formatErrorMessage(operatorType, argumentTypes, Optional.empty()), cause);
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.returnType = null;
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    public OperatorNotFoundException(OperatorType operatorType, List<? extends Type> argumentTypes, TypeSignature returnType, Throwable cause)
    {
        super(OPERATOR_NOT_FOUND, formatErrorMessage(operatorType, argumentTypes, Optional.of(returnType)), cause);
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.returnType = requireNonNull(returnType, "returnType is null");
    }

    private static String formatErrorMessage(OperatorType operatorType, List<? extends Type> argumentTypes, Optional<TypeSignature> returnType)
    {
        return switch (operatorType) {
            case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS, EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL ->
                    format("Cannot apply operator: %s %s %s", argumentTypes.get(0), operatorType.getOperator(), argumentTypes.get(1));
            case NEGATION -> format("Cannot negate %s", argumentTypes.get(0));
            case IDENTICAL -> format("Cannot check if %s is identical to %s", argumentTypes.get(0), argumentTypes.get(1));
            case CAST -> format("Cannot cast %s to %s", argumentTypes.get(0), returnType.orElseThrow());
            case SUBSCRIPT -> format("Cannot use %s for subscript of %s", argumentTypes.get(1), argumentTypes.get(0));
            default -> format(
                    "Operator '%s'%s cannot be applied to %s",
                    operatorType.getOperator(),
                    returnType.map(value -> ":" + value).orElse(""),
                    Joiner.on(", ").join(argumentTypes));
        };
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
