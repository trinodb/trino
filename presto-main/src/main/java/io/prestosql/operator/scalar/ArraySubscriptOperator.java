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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.FullConnectorSession;
import io.prestosql.SystemSessionProperties;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.SUBSCRIPT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ArraySubscriptOperator
        extends SqlOperator
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArraySubscriptOperator.class, "booleanSubscript", Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArraySubscriptOperator.class, "longSubscript", Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArraySubscriptOperator.class, "doubleSubscript", Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArraySubscriptOperator.class, "sliceSubscript", Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArraySubscriptOperator.class, "objectSubscript", Type.class, ConnectorSession.class, Block.class, long.class);

    protected ArraySubscriptOperator()
    {
        super(SUBSCRIPT,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("array(E)"), parseTypeSignature("bigint")));
    }

    private static boolean hiveEnabled(ConnectorSession session) {
        FullConnectorSession fcs = (FullConnectorSession) session;

        return fcs.getSession().getSystemProperty(SystemSessionProperties.ENABLE_HIVE_SQL_SYNTAX, Boolean.class);
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        checkArgument(boundVariables.getTypeVariables().size() == 1, "Expected one type, got %s", boundVariables.getTypeVariables());
        Type elementType = boundVariables.getTypeVariable("E");

        MethodHandle methodHandle;
        if (elementType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (elementType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (elementType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (elementType.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = methodHandle.bindTo(elementType);
        requireNonNull(methodHandle, "methodHandle is null");
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Long longSubscript(Type elementType, ConnectorSession session, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(hiveEnabled(session), array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getLong(array, position);
    }

    @UsedByGeneratedCode
    public static Boolean booleanSubscript(Type elementType, ConnectorSession session, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(hiveEnabled(session), array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(array, position);
    }

    @UsedByGeneratedCode
    public static Double doubleSubscript(Type elementType, ConnectorSession session, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(hiveEnabled(session), array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getDouble(array, position);
    }

    @UsedByGeneratedCode
    public static Slice sliceSubscript(Type elementType, ConnectorSession session, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(hiveEnabled(session), array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getSlice(array, position);
    }

    @UsedByGeneratedCode
    public static Object objectSubscript(Type elementType, ConnectorSession session, Block array, long index)
    {
        int position = checkedIndexToBlockPosition(hiveEnabled(session), array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getObject(array, position);
    }

    public static void checkArrayIndex(ConnectorSession session, long index)
    {
        if (index == 0 && !hiveEnabled(session)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (index < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array subscript is negative");
        }
    }

    private static int checkedIndexToBlockPosition(boolean enableHiveSyntax, Block block, long index)
    {
        int arrayLength = block.getPositionCount();

        if ((Math.abs(index) > arrayLength) || ((Math.abs(index) == arrayLength) && enableHiveSyntax)){
            return -1; // -1 indicates that the element is out of range and "ELEMENT_AT" should return null
        }

        if (index == 0) {
            if(!enableHiveSyntax) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Presto SQL array indices start at 1");
            }else{
                return 0;
            }
        }else if (index > 0) {
            if(!enableHiveSyntax) {
                return toIntExact(index - 1);
            }else {
                return toIntExact(index);
            }
        }
        else {
            if(!enableHiveSyntax) {
                return toIntExact(arrayLength + index + 1);
            }else {
                return toIntExact(arrayLength + index);
            }
        }
    }
}