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

public final class AddJar
        extends Statement
{
    private final String jarName;
    private final boolean notExists;

    public AddJar(String jarName, boolean notExists)
    {
        this(Optional.empty(), jarName, notExists);
    }

    public AddJar(Optional<NodeLocation> location, String jarName, boolean notExists)
    {
        super(location);
        this.jarName = jarName;
        this.notExists = notExists;
    }

    public String getJarName()
    {
        return jarName;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddJar(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        AddJar o = (AddJar) obj;
        return Objects.equals(jarName, o.jarName) && (notExists == o.notExists);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jarName, notExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("jarName", jarName).add("notExists", notExists).toString();
    }
}
