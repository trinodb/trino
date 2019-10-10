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
package io.prestosql.sql.relational;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.LogicalBinaryExpression;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.metadata.Signature.internalScalarFunction;
import static io.prestosql.metadata.Signature.mangleOperatorName;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.DIVIDE;
import static io.prestosql.spi.function.OperatorType.MODULUS;
import static io.prestosql.spi.function.OperatorType.MULTIPLY;
import static io.prestosql.spi.function.OperatorType.NEGATION;
import static io.prestosql.spi.function.OperatorType.SUBSCRIPT;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;

public final class Signatures
{
    public static final String CAST = mangleOperatorName(OperatorType.CAST);

    private Signatures()
    {
    }

    // **************** sql operators ****************
    public static Signature notSignature()
    {
        return new Signature("not", SCALAR, BOOLEAN.getTypeSignature(), ImmutableList.of(BOOLEAN.getTypeSignature()));
    }

    public static Signature betweenSignature(Type valueType, Type minType, Type maxType)
    {
        return internalOperator(BETWEEN, BOOLEAN.getTypeSignature(), valueType.getTypeSignature(), minType.getTypeSignature(), maxType.getTypeSignature());
    }

    public static Signature likeVarcharSignature()
    {
        return internalScalarFunction("LIKE", BOOLEAN.getTypeSignature(), VARCHAR.getTypeSignature(), LIKE_PATTERN.getTypeSignature());
    }

    public static Signature likeCharSignature(Type valueType)
    {
        checkArgument(valueType instanceof CharType, "Expected CHAR value type");
        return internalScalarFunction("LIKE", BOOLEAN.getTypeSignature(), valueType.getTypeSignature(), LIKE_PATTERN.getTypeSignature());
    }

    public static Signature likePatternSignature()
    {
        return internalScalarFunction("LIKE_PATTERN", LIKE_PATTERN.getTypeSignature(), VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature());
    }

    public static Signature castSignature(Type returnType, Type valueType)
    {
        // Name has already been mangled, so don't use internalOperator
        return internalScalarFunction(CAST, returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature tryCastSignature(Type returnType, Type valueType)
    {
        return internalScalarFunction("TRY_CAST", returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature logicalExpressionSignature(LogicalBinaryExpression.Operator operator)
    {
        return internalScalarFunction(operator.name(), BOOLEAN.getTypeSignature(), BOOLEAN.getTypeSignature(), BOOLEAN.getTypeSignature());
    }

    public static Signature arithmeticNegationSignature(Type returnType, Type valueType)
    {
        return internalOperator(NEGATION, returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature arithmeticExpressionSignature(ArithmeticBinaryExpression.Operator operator, Type returnType, Type leftType, Type rightType)
    {
        OperatorType operatorType;
        switch (operator) {
            case ADD:
                operatorType = ADD;
                break;
            case SUBTRACT:
                operatorType = SUBTRACT;
                break;
            case MULTIPLY:
                operatorType = MULTIPLY;
                break;
            case DIVIDE:
                operatorType = DIVIDE;
                break;
            case MODULUS:
                operatorType = MODULUS;
                break;
            default:
                throw new IllegalStateException("Unknown arithmetic operator: " + operator);
        }
        return internalOperator(operatorType, returnType.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    public static Signature subscriptSignature(Type returnType, Type leftType, Type rightType)
    {
        return internalOperator(SUBSCRIPT, returnType.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    public static Signature arrayConstructorSignature(Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(ARRAY_CONSTRUCTOR, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature));
    }

    public static Signature arrayConstructorSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(ARRAY_CONSTRUCTOR, returnType, argumentTypes);
    }

    public static Signature comparisonExpressionSignature(ComparisonExpression.Operator operator, Type leftType, Type rightType)
    {
        for (OperatorType operatorType : OperatorType.values()) {
            if (operatorType.name().equals(operator.name())) {
                return internalOperator(operatorType, BOOLEAN.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
            }
        }
        return internalScalarFunction(operator.name(), BOOLEAN.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    public static Signature trySignature(Type returnType)
    {
        return new Signature("TRY", SCALAR, returnType.getTypeSignature());
    }
}
