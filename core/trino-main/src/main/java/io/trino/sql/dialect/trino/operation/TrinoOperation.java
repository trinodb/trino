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
package io.trino.sql.dialect.trino.operation;

import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class TrinoOperation
        extends Operation
        // TODO sealed?
{
    public TrinoOperation(String dialect, String name)
    {
        super(dialect, name);
    }

    /**
     * Replace the operation's regions with newRegions.
     * The newRegions list must be the size of the operation's regions.
     * <p>
     * Currently, it is only implemented for the Lambda and  Query operations, and used in CTE reuse to rewrite nested blocks.
     * When we add optimizations for correlated queries, we will need to implement region substitution for relational operations
     * so that we can rewrite nested queries.
     */
    public Operation withRegions(List<Region> newRegions)
    {
        if (!regions().isEmpty()) {
            throw new UnsupportedOperationException(name() + " does not support region substitution");
        }
        checkArgument(newRegions.isEmpty(), "regions lists size mismatch");
        return this;
    }

    /**
     * Replace the operation's index-th argument with newArgument.
     * The newArgument must be of the same type as the current argument.
     */
    public Operation withArgument(Value newArgument, int index)
    {
        throw new UnsupportedOperationException(name() + " does not support argument substitution");
    }

    void validateArgument(Value newArgument, int index)
    {
        checkArgument(index >= 0 && index < arguments().size(), "invalid argument index");
        checkArgument(newArgument.type().equals(arguments().get(index).type()), "argument type mismatch");
    }
}
