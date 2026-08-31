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
import io.trino.metadata.TestingCatalogFunction;
import org.junit.jupiter.api.Test;

import static io.trino.metadata.TestingCatalogFunction.MULTIPLY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInterpretedFunctionInvoker
{
    @Test
    public void testCatalogFunctionReadsItsOwnCatalogSessionProperty()
    {
        InterpretedFunctionInvoker invoker = new InterpretedFunctionInvoker(TestingCatalogFunction.functionResolution().getPlannerContext().getFunctionManager());

        // the session is not bound to the function's catalog, so the invoker has to rebind it
        Object result = invoker.invoke(MULTIPLY, TestingCatalogFunction.session().toConnectorSession(), ImmutableList.of(3L));

        assertThat(result).isEqualTo(9L);
    }
}
