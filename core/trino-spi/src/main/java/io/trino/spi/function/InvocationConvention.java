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
package io.trino.spi.function;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class InvocationConvention
{
    private final List<InvocationArgumentConvention> argumentConventionList;
    private final InvocationReturnConvention returnConvention;
    private final boolean supportsSession;
    private final boolean supportsInstanceFactory;

    public static InvocationConvention simpleConvention(InvocationReturnConvention returnConvention, InvocationArgumentConvention... argumentConventions)
    {
        return new InvocationConvention(Arrays.asList(argumentConventions), returnConvention, false, false);
    }

    public InvocationConvention(
            List<InvocationArgumentConvention> argumentConventionList,
            InvocationReturnConvention returnConvention,
            boolean supportsSession,
            boolean supportsInstanceFactory)
    {
        this.argumentConventionList = List.copyOf(requireNonNull(argumentConventionList, "argumentConventionList is null"));
        this.returnConvention = returnConvention;
        this.supportsSession = supportsSession;
        this.supportsInstanceFactory = supportsInstanceFactory;
    }

    public InvocationReturnConvention getReturnConvention()
    {
        return returnConvention;
    }

    public List<InvocationArgumentConvention> getArgumentConventions()
    {
        return argumentConventionList;
    }

    public InvocationArgumentConvention getArgumentConvention(int index)
    {
        return argumentConventionList.get(index);
    }

    public boolean supportsSession()
    {
        return supportsSession;
    }

    public boolean supportsInstanceFactory()
    {
        return supportsInstanceFactory;
    }

    @Override
    public String toString()
    {
        return "(" + argumentConventionList.toString() + ")" + returnConvention;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InvocationConvention that = (InvocationConvention) o;
        return supportsSession == that.supportsSession &&
                supportsInstanceFactory == that.supportsInstanceFactory &&
                Objects.equals(argumentConventionList, that.argumentConventionList) &&
                returnConvention == that.returnConvention;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(argumentConventionList, returnConvention, supportsSession, supportsInstanceFactory);
    }

    public enum InvocationArgumentConvention
    {
        /**
         * Argument must not be a boxed type. Argument will never be null.
         */
        NEVER_NULL(false, 1),
        /**
         * Argument is passed a Block followed by the integer position in the block.
         * If the actual block position passed to the function argument is null, the
         * results are undefined.
         */
        BLOCK_POSITION_NOT_NULL(false, 2),
        /**
         * Argument is passed a ValueBlock followed by the integer position in the block.
         * The actual block parameter may be any subtype of ValueBlock, and the scalar function
         * adapter will convert the parameter to ValueBlock. If the actual block position
         * passed to the function argument is null, the results are undefined.
         */
        VALUE_BLOCK_POSITION_NOT_NULL(false, 2),
        /**
         * Argument is always an object type. An SQL null will be passed a Java null.
         */
        BOXED_NULLABLE(true, 1),
        /**
         * Argument must not be a boxed type, and is always followed with a boolean argument
         * to indicate if the sql value is null.
         */
        NULL_FLAG(true, 2),
        /**
         * Argument is passed a Block followed by the integer position in the block. The
         * sql value may be null.
         */
        BLOCK_POSITION(true, 2),
        /**
         * Argument is passed a ValueBlock followed by the integer position in the block.
         * The actual block parameter may be any subtype of ValueBlock, and the scalar function
         * adapter will convert the parameter to ValueBlock. The sql value may be null.
         */
        VALUE_BLOCK_POSITION(true, 2),
        /**
         * Argument is passed as a flat slice. The sql value may not be null.
         */
        FLAT(false, 4),
        /**
         * Argument is passed in an InOut. The sql value may be null.
         */
        IN_OUT(true, 1),
        /**
         * Argument is a lambda function.
         */
        FUNCTION(false, 1);

        private final boolean nullable;
        private final int parameterCount;

        InvocationArgumentConvention(boolean nullable, int parameterCount)
        {
            this.nullable = nullable;
            this.parameterCount = parameterCount;
        }

        public boolean isNullable()
        {
            return nullable;
        }

        public int getParameterCount()
        {
            return parameterCount;
        }
    }

    public enum InvocationReturnConvention
    {
        /**
         * The function will never return a null value.
         * It is not possible to adapt a NEVER_NULL argument to a
         * BOXED_NULLABLE or NULL_FLAG argument when this return
         * convention is used.
         */
        FAIL_ON_NULL(false, 0),
        /**
         * When a null is passed to a never null argument, the function
         * will not be invoked, and the Java default value for the return
         * type will be returned.
         * This can not be used as an actual function return convention,
         * and instead is only used for adaptation.
         */
        DEFAULT_ON_NULL(false, 0),
        /**
         * The function may return a null value.
         * When a null is passed to a never null argument, the function
         * will not be invoked, and a null value is returned.
         */
        NULLABLE_RETURN(true, 0),
        /**
         * Return value is witten to a BlockBuilder passed as the last argument.
         * When a null is passed to a never null argument, the function
         * will not be invoked, and a null is written to the block builder.
         */
        BLOCK_BUILDER(true, 1),
        /**
         * Return value is written to flat memory passed as the last 5
         * arguments to the function.
         * It is not possible to adapt a NEVER_NULL argument to a
         * BOXED_NULLABLE or NULL_FLAG argument when this return
         * convention is used.
         */
        FLAT_RETURN(false, 4),
        /**/;

        private final boolean nullable;
        private final int parameterCount;

        InvocationReturnConvention(boolean nullable, int parameterCount)
        {
            this.nullable = nullable;
            this.parameterCount = parameterCount;
        }

        public boolean isNullable()
        {
            return nullable;
        }

        public int getParameterCount()
        {
            return parameterCount;
        }
    }
}
