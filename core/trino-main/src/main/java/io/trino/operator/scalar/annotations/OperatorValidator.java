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
package io.trino.operator.scalar.annotations;

import com.google.common.base.Joiner;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.type.UnknownType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class OperatorValidator
{
    private OperatorValidator() {}

    @SuppressWarnings("RedundantLabeledSwitchRuleCodeBlock")
    public static void validateOperator(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        switch (operatorType) {
            case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS -> {
                validateOperatorSignature(operatorType, returnType, argumentTypes, 2);
            }
            case NEGATION, CAST, SATURATED_FLOOR_CAST -> {
                validateOperatorSignature(operatorType, returnType, argumentTypes, 1);
            }
            case EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL,
                 COMPARISON_UNORDERED_LAST, COMPARISON_UNORDERED_FIRST -> {
                validateComparisonOperatorSignature(operatorType, returnType, argumentTypes, 2);
            }
            case SUBSCRIPT -> {
                validateOperatorSignature(operatorType, returnType, argumentTypes, 2);
                checkArgument(argumentTypes.get(0).getBase().equals(StandardTypes.ARRAY) || argumentTypes.get(0).getBase().equals(StandardTypes.MAP) || argumentTypes.get(0).getBase().equals(StandardTypes.VARIANT), "First argument must be an ARRAY, MAP, or VARIANT");
                switch (argumentTypes.get(0).getBase()) {
                    case StandardTypes.ARRAY -> {
                        checkArgument(argumentTypes.get(1).getBase().equals(StandardTypes.BIGINT), "Second argument must be a BIGINT");
                        TypeSignature elementType = ((TypeParameter.Type) argumentTypes.get(0).getParameters().get(0)).type();
                        checkArgument(returnType.equals(elementType), "[] return type does not match ARRAY element type");
                    }
                    case StandardTypes.MAP -> {
                        TypeSignature valueType = ((TypeParameter.Type) argumentTypes.get(0).getParameters().get(1)).type();
                        checkArgument(returnType.equals(valueType), "[] return type does not match MAP value type");
                    }
                    default -> {}
                }
            }
            case HASH_CODE -> {
                validateOperatorSignature(operatorType, returnType, argumentTypes, 1);
                checkArgument(returnType.getBase().equals(StandardTypes.BIGINT), "%s operator must return a BIGINT: %s", operatorType, formatSignature(operatorType, returnType, argumentTypes));
            }
            case IDENTICAL, XX_HASH_64, INDETERMINATE, READ_VALUE -> {
                // TODO
            }
        }
    }

    private static void validateOperatorSignature(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes, int expectedArgumentCount)
    {
        String signature = formatSignature(operatorType, returnType, argumentTypes);
        checkArgument(!returnType.getBase().equals(UnknownType.NAME), "%s operator return type cannot be NULL: %s", operatorType, signature);
        checkArgument(argumentTypes.size() == expectedArgumentCount, "%s operator must have exactly %s argument: %s", operatorType, expectedArgumentCount, signature);
    }

    private static void validateComparisonOperatorSignature(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes, int expectedArgumentCount)
    {
        validateOperatorSignature(operatorType, returnType, argumentTypes, expectedArgumentCount);
        checkArgument(returnType.getBase().equals(StandardTypes.BOOLEAN), "%s operator must return a BOOLEAN: %s", operatorType, formatSignature(operatorType, returnType, argumentTypes));
    }

    private static String formatSignature(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return operatorType + "(" + Joiner.on(", ").join(argumentTypes) + ")::" + returnType;
    }
}
