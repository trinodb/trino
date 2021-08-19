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
package io.trino.sql.planner.rowpattern.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static io.trino.sql.planner.rowpattern.ir.IrAnchor.Type.PARTITION_START;
import static java.util.Objects.requireNonNull;

public class IrAnchor
        extends IrRowPattern
{
    public enum Type
    {
        PARTITION_START,
        PARTITION_END
    }

    private final Type type;

    @JsonCreator
    public IrAnchor(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrAnchor(this, context);
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
        IrAnchor o = (IrAnchor) obj;
        return Objects.equals(type, o.type);
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public String toString()
    {
        return type == PARTITION_START ? "^" : "$";
    }
}
