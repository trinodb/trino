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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record IrConstantJsonSequence(List<JsonNode> sequence, Optional<Type> type)
        implements IrPathNode
{
    public static final IrConstantJsonSequence EMPTY_SEQUENCE = new IrConstantJsonSequence(ImmutableList.of(), Optional.empty());

    public static IrConstantJsonSequence singletonSequence(JsonNode jsonNode, Optional<Type> type)
    {
        return new IrConstantJsonSequence(ImmutableList.of(jsonNode), type);
    }

    public IrConstantJsonSequence(List<JsonNode> sequence, Optional<Type> type)
    {
        this.type = requireNonNull(type, "type is null");
        this.sequence = ImmutableList.copyOf(sequence);
    }

    @Override
    public <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrConstantJsonSequence(this, context);
    }
}
