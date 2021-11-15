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
package io.trino.procedure;

import io.trino.spi.procedure.Procedure;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static java.util.Collections.emptyList;

public class TestProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle TEST_METADATA = methodHandle(TestProcedure.class, "testProcedure");

    @Override
    public Procedure get()
    {
        return new Procedure("default", "test_procedure", emptyList(), TEST_METADATA.bindTo(this));
    }

    public void testProcedure()
    {}
}
