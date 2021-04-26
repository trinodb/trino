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

import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;

import java.util.OptionalInt;

import static io.trino.sql.util.AstUtils.treeEqual;
import static io.trino.sql.util.AstUtils.treeHash;
import static java.util.Objects.requireNonNull;

public class CanonicalizationAware<T extends Node>
{
    private final T node;

    private CanonicalizationAware(T node)
    {
        this.node = requireNonNull(node, "node is null");
    }

    public static <T extends Node> CanonicalizationAware<T> canonicalizationAwareKey(T node)
    {
        return new CanonicalizationAware<T>(node);
    }

    public T getNode()
    {
        return node;
    }

    @Override
    public int hashCode()
    {
        return treeHash(node, CanonicalizationAware::canonicalizationAwareHash);
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

        CanonicalizationAware<T> other = (CanonicalizationAware<T>) o;
        return treeEqual(node, other.node, CanonicalizationAware::canonicalizationAwareComparison);
    }

    @Override
    public String toString()
    {
        return "CanonicalizationAware(" + node + ")";
    }

    public static Boolean canonicalizationAwareComparison(Node left, Node right)
    {
        if (left instanceof Identifier && right instanceof Identifier) {
            Identifier leftIdentifier = (Identifier) left;
            Identifier rightIdentifier = (Identifier) right;

            return leftIdentifier.getCanonicalValue().equals(rightIdentifier.getCanonicalValue());
        }

        return null;
    }

    public static OptionalInt canonicalizationAwareHash(Node node)
    {
        if (node instanceof Identifier) {
            return OptionalInt.of(((Identifier) node).getCanonicalValue().hashCode());
        }
        else if (node.getChildren().isEmpty()) {
            return OptionalInt.of(node.hashCode());
        }
        return OptionalInt.empty();
    }
}
