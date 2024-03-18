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
package io.trino.plugin.varada.dispatcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

public class SingleValue
{
    private final Type type;

    // We use block the same way io.trino.spi.predicate.Marker does, to avoid Json deserialization issues
    // value within the block can be null (in case of "WHERE col1 is null")
    private final Block valueBlock;

    // Cache the value, in order not to read the block more than once
    private Object value;
    private boolean isConverted;

    @JsonCreator
    public SingleValue(@JsonProperty("type") Type type,
            @JsonProperty("valueBlock") Block valueBlock)
    {
        this(type, null, valueBlock, false);
    }

    private SingleValue(Type type, Object value, Block valueBlock, boolean isConverted)
    {
        this.type = requireNonNull(type, "type is null");

        this.valueBlock = requireNonNull(valueBlock, "valueBlock is null");
        checkArgument(valueBlock.getPositionCount() == 1, "value block should only have one position");

        this.value = value;
        this.isConverted = isConverted;
    }

    public static SingleValue create(Type type, Object value)
    {
        if (value instanceof NullableValue) {
            value = ((NullableValue) value).getValue();
        }
        return new SingleValue(type, value, nativeValueToBlock(type, value), true);
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Block getValueBlock()
    {
        return valueBlock;
    }

    @JsonIgnore
    public Object getValue()
    {
        if (!isConverted) {
            value = readNativeValue(type, valueBlock, 0);
            isConverted = true;
        }
        return value;
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
        SingleValue that = (SingleValue) o;

        // Notice: valueBlock can't be compared unless implementing equalOperator (see io.trino.spi.predicate.Marker)
        return Objects.equals(type, that.type) &&
                Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode()
    {
        // Notice: valueBlock can't be hashed unless implementing hashCodeOperator (see io.trino.spi.predicate.Marker)
        return Objects.hash(type, getValue());
    }

    @Override
    public String toString()
    {
        return "SingleValue{" +
                "type=" + type +
                ", value=" + getValueImpl() +
                '}';
    }

    private String getValueImpl()
    {
        String ret = "n/a";
        if (isConverted) {
            ret = String.valueOf(value);
        }
        return ret;
    }
}
