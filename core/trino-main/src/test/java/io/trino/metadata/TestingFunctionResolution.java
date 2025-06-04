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

import io.trino.Session;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.Plugin;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.gen.CursorProcessorCompiler;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.TestingPlannerContext;
import io.trino.testing.QueryRunner;
import io.trino.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Objects.requireNonNull;

public class TestingFunctionResolution
{
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final PlannerContext plannerContext;

    public TestingFunctionResolution()
    {
        this(new InternalFunctionBundle());
    }

    public TestingFunctionResolution(FunctionBundle functions)
    {
        transactionManager = createTestTransactionManager();
        plannerContext = plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .addFunctions(functions)
                .build();
        metadata = plannerContext.getMetadata();
    }

    public TestingFunctionResolution(Plugin plugin)
    {
        transactionManager = createTestTransactionManager();

        TestingPlannerContext.Builder builder = plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .addFunctions(extractFunctions(plugin.getFunctions()));

        plugin.getTypes().forEach(builder::addType);
        plugin.getParametricTypes().forEach(builder::addParametricType);

        plannerContext = builder.build();
        metadata = plannerContext.getMetadata();
    }

    public TestingFunctionResolution(QueryRunner queryRunner)
    {
        this(queryRunner.getTransactionManager(), queryRunner.getPlannerContext());
    }

    public TestingFunctionResolution(TransactionManager transactionManager, PlannerContext plannerContext)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
    }

    public PlannerContext getPlannerContext()
    {
        return plannerContext;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public ExpressionCompiler getExpressionCompiler()
    {
        return new ExpressionCompiler(getCursorProcessorCompiler(), getPageFunctionCompiler(), getColumnarFilterCompiler());
    }

    public CursorProcessorCompiler getCursorProcessorCompiler()
    {
        return new CursorProcessorCompiler(plannerContext.getFunctionManager());
    }

    public PageFunctionCompiler getPageFunctionCompiler()
    {
        return getPageFunctionCompiler(0);
    }

    public PageFunctionCompiler getPageFunctionCompiler(int expressionCacheSize)
    {
        return new PageFunctionCompiler(plannerContext.getFunctionManager(), expressionCacheSize);
    }

    public Collection<FunctionMetadata> listGlobalFunctions()
    {
        return inTransaction(metadata::listGlobalFunctions);
    }

    public ColumnarFilterCompiler getColumnarFilterCompiler()
    {
        return getColumnarFilterCompiler(0);
    }

    public ColumnarFilterCompiler getColumnarFilterCompiler(int expressionCacheSize)
    {
        return new ColumnarFilterCompiler(plannerContext.getFunctionManager(), expressionCacheSize);
    }

    public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return inTransaction(session -> metadata.resolveOperator(operatorType, argumentTypes));
    }

    public ResolvedFunction getCoercion(Type fromType, Type toType)
    {
        return inTransaction(session -> metadata.getCoercion(fromType, toType));
    }

    public ResolvedFunction getCoercion(CatalogSchemaFunctionName name, Type fromType, Type toType)
    {
        return inTransaction(session -> metadata.getCoercion(name, fromType, toType));
    }

    public TestingFunctionCallBuilder functionCallBuilder(String name)
    {
        return new TestingFunctionCallBuilder(name);
    }

    //
    // Resolving or fetching a function in a transaction and then using that in another transaction, is not
    // legal, but works for tests
    //

    public ResolvedFunction resolveFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        return metadata.resolveBuiltinFunction(name, parameterTypes);
    }

    public TestingAggregationFunction getAggregateFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        return inTransaction(session -> {
            ResolvedFunction resolvedFunction = metadata.resolveBuiltinFunction(name, parameterTypes);
            return new TestingAggregationFunction(
                    resolvedFunction.signature(),
                    resolvedFunction.functionNullability(),
                    plannerContext.getFunctionManager().getAggregationImplementation(resolvedFunction));
        });
    }

    private <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }

    public class TestingFunctionCallBuilder
    {
        private final String name;
        private List<TypeSignature> argumentTypes = new ArrayList<>();
        private List<Expression> argumentValues = new ArrayList<>();

        public TestingFunctionCallBuilder(String name)
        {
            this.name = name;
        }

        public TestingFunctionCallBuilder addArgument(Type type, Expression value)
        {
            requireNonNull(type, "type is null");
            return addArgument(type.getTypeSignature(), value);
        }

        public TestingFunctionCallBuilder addArgument(TypeSignature typeSignature, Expression value)
        {
            requireNonNull(typeSignature, "typeSignature is null");
            requireNonNull(value, "value is null");
            argumentTypes.add(typeSignature);
            argumentValues.add(value);
            return this;
        }

        public TestingFunctionCallBuilder setArguments(List<Type> types, List<Expression> values)
        {
            requireNonNull(types, "types is null");
            requireNonNull(values, "values is null");
            argumentTypes = types.stream()
                    .map(Type::getTypeSignature)
                    .collect(Collectors.toList());
            argumentValues = new ArrayList<>(values);
            return this;
        }

        public Call build()
        {
            return new Call(
                    resolveFunction(name, TypeSignatureProvider.fromTypeSignatures(argumentTypes)),
                    argumentValues);
        }
    }
}
