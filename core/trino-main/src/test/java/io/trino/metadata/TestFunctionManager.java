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
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.InvocationConvention;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;

import static io.trino.metadata.FunctionManager.isConnectorFunction;
import static io.trino.metadata.TestingCatalogFunction.CATALOG_HANDLE;
import static io.trino.metadata.TestingCatalogFunction.MULTIPLY;
import static io.trino.operator.scalar.SessionFunctions.CURRENT_USER_FUNCTION_NAME;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFunctionManager
{
    @Test
    public void testConnectorFunctionRequiresSessionBoundToItsCatalog()
            throws Throwable
    {
        FunctionManager functionManager = TestingCatalogFunction.functionResolution().getPlannerContext().getFunctionManager();
        MethodHandle method = functionManager.getScalarFunctionImplementation(MULTIPLY, new InvocationConvention(ImmutableList.of(NEVER_NULL), FAIL_ON_NULL, true, false)).getMethodHandle();

        assertThatThrownBy(() -> method.invoke(TestingCatalogFunction.session().toConnectorSession(), 3L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("ConnectorSession is bound to null, but the function is from");
        assertThat((long) method.invoke(TestingCatalogFunction.session().toConnectorSession(CATALOG_HANDLE), 3L)).isEqualTo(9L);
    }

    @Test
    public void testOnlyCatalogFunctionsAreConnectorFunctions()
    {
        // LanguageFunctionManager gives stored routines function ids with this prefix
        ResolvedFunction storedRoutine = new ResolvedFunction(
                MULTIPLY.signature(),
                CATALOG_HANDLE,
                new FunctionId("$trino_sql_routine"),
                SCALAR,
                true,
                false,
                MULTIPLY.functionNullability(),
                MULTIPLY.typeDependencies(),
                MULTIPLY.functionDependencies());
        ResolvedFunction builtIn = TestingCatalogFunction.functionResolution().resolveFunction(CURRENT_USER_FUNCTION_NAME, ImmutableList.of());

        assertThat(isConnectorFunction(MULTIPLY)).isTrue();
        assertThat(isConnectorFunction(storedRoutine)).isFalse();
        assertThat(isConnectorFunction(builtIn)).isFalse();
    }
}
