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

public class ShowGrants
        extends Statement
{
    private final Optional<GrantObject> grantObject;

    public ShowGrants(Optional<GrantObject> grantObject)
    {
        this(Optional.empty(), grantObject);
    }

    public ShowGrants(NodeLocation location, Optional<GrantObject> grantObject)
    {
        this(Optional.of(location), grantObject);
    }

    public ShowGrants(Optional<NodeLocation> location, Optional<GrantObject> grantObject)
    {
        super(location);
        this.grantObject = grantObject == null ? Optional.empty() : grantObject;
    }

    public Optional<String> getEntityKind()
    {
        return grantObject.flatMap(GrantObject::getEntityKind);
    }

    public Optional<GrantObject> getGrantObject()
    {
        return grantObject;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowGrants(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return grantObject.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowGrants o = (ShowGrants) obj;
        return Objects.equals(grantObject, o.grantObject);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("grantScope", grantObject)
                .toString();
    }
}
