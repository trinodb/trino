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
package io.trino.newir.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record BlockNode(Optional<String> name, List<String> parameterNames, List<TypeNode> parameterTypes, List<OperationNode> operations)
        implements NewIrNode
{
    public BlockNode(Optional<String> name, List<String> parameterNames, List<TypeNode> parameterTypes, List<OperationNode> operations)
    {
        requireNonNull(name, "name is null");
        requireNonNull(parameterNames, "parameterNames is null");
        requireNonNull(parameterTypes, "parameterTypes is null");
        requireNonNull(operations, "operations is null");

        checkArgument(parameterNames.size() == parameterTypes.size(), "parameter names and parameter types lists do not match in size");
        checkArgument(!operations.isEmpty(), "operations list is empty");

        this.name = name;
        this.parameterNames = ImmutableList.copyOf(parameterNames);
        this.parameterTypes = ImmutableList.copyOf(parameterTypes);
        this.operations = ImmutableList.copyOf(operations);
    }
}
