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

import io.prestosql.Session;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.type.TypeRegistry;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

@ThreadSafe
public class FunctionManager
{
    private final FunctionRegistry functionRegistry;
    private final FunctionInvokerProvider functionInvokerProvider;

    public FunctionManager(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, FeaturesConfig featuresConfig)
    {
        this.functionRegistry = new FunctionRegistry(typeManager, blockEncodingSerde, featuresConfig, this);
        this.functionInvokerProvider = new FunctionInvokerProvider(functionRegistry);
        if (typeManager instanceof TypeRegistry) {
            ((TypeRegistry) typeManager).setFunctionManager(this);
        }
    }

    public FunctionInvokerProvider getFunctionInvokerProvider()
    {
        return functionInvokerProvider;
    }

    public void addFunctions(List<? extends SqlFunction> functions)
    {
        functionRegistry.addFunctions(functions);
    }

    public List<SqlFunction> listFunctions()
    {
        return functionRegistry.list();
    }

    public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return functionRegistry.resolveFunction(name, parameterTypes).getSignature();
    }

    public FunctionHandle resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return functionRegistry.resolveFunction(name, parameterTypes);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        return functionRegistry.getWindowFunctionImplementation(functionHandle);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        return functionRegistry.getAggregateFunctionImplementation(functionHandle);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        return functionRegistry.getScalarFunctionImplementation(functionHandle);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        return functionRegistry.getScalarFunctionImplementation(new FunctionHandle(signature));
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return functionRegistry.isAggregationFunction(name);
    }

    public FunctionHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        return functionRegistry.resolveOperator(operatorType, argumentTypes);
    }

    public FunctionHandle lookupCast(TypeSignature fromType, TypeSignature toType)
    {
        return functionRegistry.lookupCast(fromType, toType);
    }

    public FunctionHandle lookupSaturatedFloorCast(TypeSignature fromType, TypeSignature toType)
    {
        return functionRegistry.lookupSaturatedFloorCast(fromType, toType);
    }

    public boolean isRegistered(Signature signature)
    {
        return functionRegistry.isRegistered(signature);
    }
}
