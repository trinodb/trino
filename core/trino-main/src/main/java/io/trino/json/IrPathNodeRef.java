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
package io.trino.json;

import io.trino.json.ir.IrPathNode;

import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public final class IrPathNodeRef<T extends IrPathNode>
{
    public static <T extends IrPathNode> IrPathNodeRef<T> of(T pathNode)
    {
        return new IrPathNodeRef<>(pathNode);
    }

    private final T pathNode;

    private IrPathNodeRef(T pathNode)
    {
        this.pathNode = requireNonNull(pathNode, "pathNode is null");
    }

    public T getNode()
    {
        return pathNode;
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
        IrPathNodeRef<?> other = (IrPathNodeRef<?>) o;
        return pathNode == other.pathNode;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(pathNode);
    }

    @Override
    public String toString()
    {
        return format(
                "@%s: %s",
                Integer.toHexString(identityHashCode(pathNode)),
                pathNode);
    }
}
