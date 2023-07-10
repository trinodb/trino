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
package io.trino.sql.analyzer;

import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;

public class RelationId
{
    /**
     * Creates {@link RelationId} equal to any {@link RelationId} created from exactly the same source.
     */
    public static RelationId of(Node sourceNode)
    {
        return new RelationId(Optional.of(NodeRef.of(sourceNode)));
    }

    /**
     * Creates {@link RelationId} equal only to itself
     */
    public static RelationId anonymous()
    {
        return new RelationId(Optional.empty());
    }

    private final Optional<NodeRef<Node>> sourceNode;

    private RelationId(Optional<NodeRef<Node>> sourceNode)
    {
        this.sourceNode = sourceNode;
    }

    public boolean isAnonymous()
    {
        return sourceNode.isEmpty();
    }

    public Optional<Node> getSourceNode()
    {
        return sourceNode.map(NodeRef::getNode);
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
        RelationId that = (RelationId) o;
        return sourceNode.isPresent() && that.sourceNode.isPresent() && sourceNode.equals(that.sourceNode);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceNode);
    }

    @Override
    public String toString()
    {
        if (isAnonymous()) {
            return toStringHelper(this)
                    .addValue("anonymous")
                    .addValue(format("x%08x", identityHashCode(this)))
                    .toString();
        }
        return toStringHelper(this)
                .addValue(sourceNode.get().getClass().getSimpleName())
                .addValue(format("x%08x", identityHashCode(sourceNode.get())))
                .toString();
    }
}
