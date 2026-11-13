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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingCatalogFunction;
import io.trino.metadata.TestingCatalogFunction.BindingCountingSession;
import io.trino.metadata.TestingFunctionResolution;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.TestingCatalogFunction.MULTIPLY;
import static io.trino.operator.scalar.SessionFunctions.CURRENT_USER_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInterpretedFunctionInvoker
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = TestingCatalogFunction.functionResolution();
    private static final InterpretedFunctionInvoker INVOKER = new InterpretedFunctionInvoker(FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager());

    @Test
    public void testCatalogFunctionReadsItsOwnCatalogSessionProperty()
    {
        BindingCountingSession session = new BindingCountingSession(TestingCatalogFunction.session());

        // the session is not bound to the function's catalog, so the invoker has to rebind it
        assertThat(INVOKER.invoke(MULTIPLY, session, ImmutableList.of(3L))).isEqualTo(9L);
        assertThat(session.getBindings()).isEqualTo(1);
    }

    @Test
    public void testBuiltInFunctionDoesNotBindSession()
    {
        BindingCountingSession session = new BindingCountingSession(TestingCatalogFunction.session());
        ResolvedFunction currentUser = FUNCTION_RESOLUTION.resolveFunction(CURRENT_USER_FUNCTION_NAME, ImmutableList.of());

        assertThat(INVOKER.invoke(currentUser, session, ImmutableList.of())).isEqualTo(utf8Slice(session.getUser()));
        assertThat(session.getBindings()).isEqualTo(0);
    }
}
