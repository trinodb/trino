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
package io.trino.operator.scalar;

import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.READ_VALUE;

@ScalarFunction("element")
@Description("Returns the sole element of a multiset that has exactly one element")
public final class MultisetElementFunction
{
    private MultisetElementFunction() {}

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Object element(
            @OperatorDependency(operator = READ_VALUE, argumentTypes = "E", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL)) MethodHandle readValue,
            @SqlType("multiset(E)") SqlMultiset multiset)
            throws Throwable
    {
        int count = multiset.getSize();
        if (count > 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cardinality violation: multiset passed to ELEMENT has more than one element");
        }
        if (count == 0 || multiset.isElementNull(0)) {
            return null;
        }
        return readValue.invoke(multiset.getElementBlock(), 0);
    }
}
