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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class CreateFunction
        extends Statement
{
    private final FunctionSpecification specification;
    private final boolean replace;

    public CreateFunction(FunctionSpecification specification, boolean replace)
    {
        this(Optional.empty(), specification, replace);
    }

    public CreateFunction(NodeLocation location, FunctionSpecification specification, boolean replace)
    {
        this(Optional.of(location), specification, replace);
    }

    private CreateFunction(Optional<NodeLocation> location, FunctionSpecification specification, boolean replace)
    {
        super(location);
        this.specification = requireNonNull(specification, "specification is null");
        this.replace = replace;
    }

    public FunctionSpecification getSpecification()
    {
        return specification;
    }

    public boolean isReplace()
    {
        return replace;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateFunction(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(specification);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof CreateFunction other) &&
                Objects.equals(specification, other.specification) &&
                Objects.equals(replace, other.replace);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(specification, replace);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specification", specification)
                .add("replace", replace)
                .toString();
    }
}
