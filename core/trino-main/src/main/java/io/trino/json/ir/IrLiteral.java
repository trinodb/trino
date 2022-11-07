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
package io.trino.json.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

public class IrLiteral
        extends IrPathNode
{
    // (boxed) native representation
    private final Object value;

    public IrLiteral(Type type, Object value)
    {
        super(Optional.of(type));
        this.value = requireNonNull(value, "value is null"); // no null values allowed
    }

    @Deprecated // For JSON deserialization only
    @JsonCreator
    public static IrLiteral fromJson(@JsonProperty("type") Type type, @JsonProperty("valueAsBlock") Block value)
    {
        checkArgument(value.getPositionCount() == 1);
        return new IrLiteral(type, readNativeValue(type, value, 0));
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrLiteral(this, context);
    }

    @JsonIgnore
    public Object getValue()
    {
        return value;
    }

    @JsonProperty
    public Block getValueAsBlock()
    {
        return nativeValueToBlock(getType().orElseThrow(), value);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IrLiteral other = (IrLiteral) obj;
        return Objects.equals(this.value, other.value) && Objects.equals(this.getType(), other.getType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, getType());
    }
}
