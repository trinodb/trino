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
package io.prestosql.spi.function;

import java.util.List;

public class InvocationConvention
{
    private final List<InvocationArgumentConvention> argumentConventionList;
    private final InvocationReturnConvention returnConvention;
    private final boolean supportsSession;
    private final boolean supportsInstanceFactory;

    public InvocationConvention(
            List<InvocationArgumentConvention> argumentConventionList,
            InvocationReturnConvention returnConvention,
            boolean supportsSession,
            boolean supportsInstanceFactory)
    {
        this.argumentConventionList = argumentConventionList;
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

    public boolean supportsInstanceFactor()
    {
        return supportsInstanceFactory;
    }

    @Override
    public String toString()
    {
        return "(" + argumentConventionList.toString() + ")" + returnConvention;
    }

    public enum InvocationArgumentConvention
    {
        /**
         * Argument must not be a boxed type. Argument will never be null.
         */
        NEVER_NULL,
        /**
         * Argument is always an object type. A SQL null will be passed a Java null.
         */
        BOXED_NULLABLE,
        /**
         * Argument must not be a boxed type, and is always followed with a boolean argument
         * to indicate if the sql value is null.
         */
        NULL_FLAG,
        /**
         * Argument is passed a Block followed by the integer position in the block.  The
         * sql value may be null.
         */
        BLOCK_POSITION,
        /**
         * Argument is a lambda function.
         */
        FUNCTION
    }

    public enum InvocationReturnConvention
    {
        FAIL_ON_NULL,
        NULLABLE_RETURN
    }
}
