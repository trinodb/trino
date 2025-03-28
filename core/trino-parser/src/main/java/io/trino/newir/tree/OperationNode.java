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

public record OperationNode(
        Optional<String> dialect,
        String name,
        String resultName,
        TypeNode resultType,
        List<String> argumentNames,
        List<TypeNode> argumentTypes,
        List<RegionNode> regions,
        List<AttributeNode> attributes)
        implements NewIrNode
{
    public OperationNode(Optional<String> dialect, String name, String resultName, TypeNode resultType, List<String> argumentNames, List<TypeNode> argumentTypes, List<RegionNode> regions, List<AttributeNode> attributes)
    {
        requireNonNull(dialect, "dialect is null");
        requireNonNull(name, "name is null");
        requireNonNull(resultName, "resultName is null");
        requireNonNull(resultType, "resultType is null");
        requireNonNull(argumentNames, "argumentNames is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        requireNonNull(regions, "regions is null");
        requireNonNull(attributes, "attributes is null");

        checkArgument(argumentNames.size() == argumentTypes.size(), "argument names and argument types lists do not match in size");

        this.dialect = dialect;
        this.name = name;
        this.resultName = resultName;
        this.resultType = resultType;
        this.argumentNames = ImmutableList.copyOf(argumentNames);
        this.argumentTypes = ImmutableList.copyOf(argumentTypes);
        this.regions = ImmutableList.copyOf(regions);
        this.attributes = ImmutableList.copyOf(attributes);
    }
}
