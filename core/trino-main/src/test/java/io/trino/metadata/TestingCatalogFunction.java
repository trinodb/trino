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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.TestingPlannerContext;
import io.trino.transaction.TransactionManager;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.util.Reflection.methodHandle;

/**
 * A catalog-scoped scalar function that reads a catalog session property, for testing that every
 * invocation path binds the {@link ConnectorSession} to the function's own catalog.
 */
public final class TestingCatalogFunction
{
    public static final String CATALOG_NAME = "mock";
    public static final String MULTIPLIER_PROPERTY = "connector_long";
    public static final long MULTIPLIER = 3;

    public static final CatalogHandle CATALOG_HANDLE = createRootCatalogHandle(new CatalogName(CATALOG_NAME), new CatalogVersion("test"));

    public static final ResolvedFunction MULTIPLY = new ResolvedFunction(
            new BoundSignature(new CatalogSchemaFunctionName(CATALOG_NAME, "default", "multiply"), BIGINT, ImmutableList.of(BIGINT)),
            CATALOG_HANDLE,
            new FunctionId("multiply"),
            SCALAR,
            true,
            false,
            new FunctionNullability(false, ImmutableList.of(false)),
            ImmutableMap.of(),
            ImmutableSet.of());

    public static final ResolvedFunction EXCEEDS_MULTIPLIER = new ResolvedFunction(
            new BoundSignature(new CatalogSchemaFunctionName(CATALOG_NAME, "default", "exceeds_multiplier"), BOOLEAN, ImmutableList.of(BIGINT)),
            CATALOG_HANDLE,
            new FunctionId("exceeds_multiplier"),
            SCALAR,
            true,
            false,
            new FunctionNullability(false, ImmutableList.of(false)),
            ImmutableMap.of(),
            ImmutableSet.of());

    private static final MethodHandle MULTIPLY_HANDLE = methodHandle(TestingCatalogFunction.class, "multiply", ConnectorSession.class, long.class);
    private static final MethodHandle EXCEEDS_MULTIPLIER_HANDLE = methodHandle(TestingCatalogFunction.class, "exceedsMultiplier", ConnectorSession.class, long.class);
    private static final InvocationConvention ACTUAL_CONVENTION = new InvocationConvention(ImmutableList.of(NEVER_NULL), FAIL_ON_NULL, true, false);

    private TestingCatalogFunction() {}

    public static TestingFunctionResolution functionResolution()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        return new TestingFunctionResolution(transactionManager, plannerContext(transactionManager));
    }

    public static PlannerContext plannerContext(TransactionManager transactionManager)
    {
        return TestingPlannerContext.plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .withFunctionProviders(CatalogServiceProvider.singleton(CATALOG_HANDLE, new FunctionProvider()
                {
                    @Override
                    public ScalarFunctionImplementation getScalarFunctionImplementation(
                            FunctionId functionId,
                            BoundSignature boundSignature,
                            FunctionDependencies functionDependencies,
                            InvocationConvention invocationConvention)
                    {
                        return ScalarFunctionImplementation.builder()
                                .methodHandle(ScalarFunctionAdapter.adapt(
                                        boundSignature.getName().functionName().equals("multiply") ? MULTIPLY_HANDLE : EXCEEDS_MULTIPLIER_HANDLE,
                                        boundSignature.getReturnType(),
                                        boundSignature.getArgumentTypes(),
                                        ACTUAL_CONVENTION,
                                        invocationConvention))
                                .build();
                    }
                }))
                .build();
    }

    /**
     * A session whose current catalog is not {@link #CATALOG_NAME}, so the connector session it
     * produces is not bound to the function's catalog.
     */
    public static Session session()
    {
        Map<String, PropertyMetadata<?>> properties = ImmutableMap.of(MULTIPLIER_PROPERTY, longProperty(MULTIPLIER_PROPERTY, "Multiplier", 1L, false));
        return testSessionBuilder(new SessionPropertyManager(
                ImmutableSet.of(new SystemSessionProperties()),
                CatalogServiceProvider.singleton(CATALOG_HANDLE, properties)))
                .setCatalog("tpch")
                .setCatalogSessionProperty(CATALOG_NAME, MULTIPLIER_PROPERTY, Long.toString(MULTIPLIER))
                .build();
    }

    public static Expression multiplyCall(Expression argument)
    {
        return new Call(MULTIPLY, ImmutableList.of(argument));
    }

    public static long multiply(ConnectorSession session, long value)
    {
        return value * session.getProperty(MULTIPLIER_PROPERTY, Long.class);
    }

    public static boolean exceedsMultiplier(ConnectorSession session, long value)
    {
        return value > session.getProperty(MULTIPLIER_PROPERTY, Long.class);
    }
}
