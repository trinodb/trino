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
package io.trino.type;

import org.testng.annotations.Test;

import java.lang.invoke.MethodType;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

import static io.trino.util.SingleAccessMethodCompiler.compileSingleAccessMethod;
import static java.lang.invoke.MethodHandles.lookup;
import static org.testng.Assert.assertEquals;

public class TestSingleAccessMethodCompiler
{
    @Test
    public void testBasic()
            throws ReflectiveOperationException
    {
        LongUnaryOperator addOne = compileSingleAccessMethod(
                "AddOne",
                LongUnaryOperator.class,
                lookup().findStatic(TestSingleAccessMethodCompiler.class, "increment", MethodType.methodType(long.class, long.class)));
        assertEquals(addOne.applyAsLong(1), 2L);
    }

    private static long increment(long x)
    {
        return x + 1;
    }

    @Test
    public void testGeneric()
            throws ReflectiveOperationException
    {
        @SuppressWarnings("unchecked")
        LongFunction<String> print = (LongFunction<String>) compileSingleAccessMethod(
                "Print",
                LongFunction.class,
                lookup().findStatic(TestSingleAccessMethodCompiler.class, "incrementAndPrint", MethodType.methodType(String.class, long.class)));
        assertEquals(print.apply(1), "2");
    }

    private static String incrementAndPrint(long x)
    {
        return String.valueOf(x + 1);
    }
}
