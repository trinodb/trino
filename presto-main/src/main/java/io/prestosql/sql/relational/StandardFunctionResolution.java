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

import io.prestosql.Session;
import io.prestosql.metadata.FunctionHandle;
import io.prestosql.metadata.FunctionManager;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.QualifiedName;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.DIVIDE;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.MODULUS;
import static io.prestosql.spi.function.OperatorType.MULTIPLY;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static java.util.Objects.requireNonNull;

public final class StandardFunctionResolution
{
    private final Session session;
    private final FunctionManager functionManager;

    public StandardFunctionResolution(Session session, FunctionManager functionManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    public FunctionHandle notFunction()
    {
        return functionManager.resolveFunction(session, QualifiedName.of("not"), fromTypes(BOOLEAN));
    }

    public FunctionHandle likeVarcharSignature()
    {
        return functionManager.resolveFunction(session, QualifiedName.of("LIKE"), fromTypes(VARCHAR, LIKE_PATTERN));
    }

    public FunctionHandle likeCharFunction(Type valueType)
    {
        checkArgument(valueType instanceof CharType, "Expected CHAR value type");
        return functionManager.resolveFunction(session, QualifiedName.of("LIKE"), fromTypes(valueType, LIKE_PATTERN));
    }

    public FunctionHandle likePatternFunction()
    {
        return functionManager.resolveFunction(session, QualifiedName.of("LIKE_PATTERN"), fromTypes(VARCHAR, VARCHAR));
    }

//    public FunctionHandle tryCastFunction(Type returnType, Type valueType)
//    {
//        return internalScalarFunction("TRY_CAST", returnType.getTypeSignature(), valueType.getTypeSignature());
//    }

    public FunctionHandle arithmeticFunction(ArithmeticBinaryExpression.Operator operator, Type leftType, Type rightType)
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
        return functionManager.resolveOperator(operatorType, fromTypes(leftType, rightType));
    }

    public FunctionHandle arrayConstructor(List<? extends Type> argumentTypes)
    {
        return functionManager.resolveFunction(session, QualifiedName.of(ARRAY_CONSTRUCTOR), fromTypes(argumentTypes));
    }

    public FunctionHandle comparisonFunction(ComparisonExpression.Operator operator, Type leftType, Type rightType)
    {
        OperatorType operatorType;
        switch (operator) {
            case EQUAL:
                operatorType = EQUAL;
                break;
            case NOT_EQUAL:
                operatorType = NOT_EQUAL;
                break;
            case LESS_THAN:
                operatorType = LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                operatorType = LESS_THAN_OR_EQUAL;
                break;
            case GREATER_THAN:
                operatorType = GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                operatorType = GREATER_THAN_OR_EQUAL;
                break;
            case IS_DISTINCT_FROM:
                operatorType = IS_DISTINCT_FROM;
                break;
            default:
                throw new IllegalStateException("Unsupported comparison operator type: " + operator);
        }

        return functionManager.resolveOperator(operatorType, fromTypes(leftType, rightType));
    }

    public FunctionHandle tryFunction(Type returnType)
    {
        return functionManager.resolveFunction(session, QualifiedName.of("TRY"), fromTypes(returnType));
    }
}
