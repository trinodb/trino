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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record IrArrayAccessor(IrPathNode base, List<Subscript> subscripts, Optional<Type> type)
        implements IrPathNode
{
    public IrArrayAccessor(IrPathNode base, List<Subscript> subscripts, Optional<Type> type)
    {
        this.type = requireNonNull(type, "type is null");
        this.base = requireNonNull(base, "array accessor base is null");
        this.subscripts = ImmutableList.copyOf(subscripts);
    }

    @Override
    public <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrArrayAccessor(this, context);
    }

    public record Subscript(IrPathNode from, Optional<IrPathNode> to)
    {
        public Subscript
        {
            requireNonNull(from, "from is null");
            requireNonNull(to, "to is null");
        }
    }
}
