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
package io.trino.spi.procedure;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static org.assertj.core.api.Assertions.assertThat;

final class TestProcedure
{
    private static final MethodHandle NOOP_METHOD_HANDLE;

    static {
        try {
            NOOP_METHOD_HANDLE = MethodHandles.lookup().findStatic(TestProcedure.class, "noop", MethodType.methodType(void.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void noop() {}

    @Test
    void testDefaultsToCoordinatorExecution()
    {
        Procedure fourArg = new Procedure("schema", "name", ImmutableList.of(), NOOP_METHOD_HANDLE);
        assertThat(fourArg.executesOnWorker()).isFalse();

        Procedure fiveArg = new Procedure("schema", "name", ImmutableList.of(), NOOP_METHOD_HANDLE, true);
        assertThat(fiveArg.executesOnWorker()).isFalse();
        assertThat(fiveArg.requiresNamedArguments()).isTrue();
    }

    @Test
    void testExecuteOnWorkerOptIn()
    {
        Procedure procedure = new Procedure("schema", "name", ImmutableList.of(), NOOP_METHOD_HANDLE, false, true);
        assertThat(procedure.executesOnWorker()).isTrue();
    }
}
