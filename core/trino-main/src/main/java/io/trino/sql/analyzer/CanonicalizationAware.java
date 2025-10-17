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
    private final boolean ignoreCase;

    // Updates to this field are thread-safe despite benign data race due to:
    // 1. idempotent hash computation
    // 2. atomic updates to int fields per JMM
    private int hashCode;

    private CanonicalizationAware(T node, boolean ignoreCase)
    {
        this.node = requireNonNull(node, "node is null");
        this.ignoreCase = ignoreCase;
    }

    public static <T extends Node> CanonicalizationAware<T> canonicalizationAwareKey(T node)
    {
        return new CanonicalizationAware<T>(node, false);
    }

    public static <T extends Node> CanonicalizationAware<T> canonicalizationAwareKey(T node, boolean ignoreCase)
    {
        return new CanonicalizationAware<T>(node, ignoreCase);
    }

    public T getNode()
    {
        return node;
    }

    @Override
    public int hashCode()
    {
        int hash = hashCode;
        if (hash == 0) {
            hash = treeHash(node,
                    ignoreCase ? CanonicalizationAware::canonicalizationAwareIgnoreCaseHash : CanonicalizationAware::canonicalizationAwareHash);
            if (hash == 0) {
                hash = 1;
            }

            hashCode = hash;
        }

        return hash;
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
        return treeEqual(node, other.node,
                ignoreCase ? CanonicalizationAware::canonicalizationAwareIgnoreCaseComparison : CanonicalizationAware::canonicalizationAwareComparison);
    }

    @Override
    public String toString()
    {
        return "CanonicalizationAware(" + node + ")";
    }

    public static Boolean canonicalizationAwareComparison(Node left, Node right)
    {
        if (left instanceof Identifier leftIdentifier && right instanceof Identifier rightIdentifier) {
            return leftIdentifier.getCanonicalValue().equals(rightIdentifier.getCanonicalValue());
        }

        return null;
    }

    public static Boolean canonicalizationAwareIgnoreCaseComparison(Node left, Node right)
    {
        if (left instanceof Identifier leftIdentifier && right instanceof Identifier rightIdentifier) {
            return leftIdentifier.getCanonicalValue(true).equals(rightIdentifier.getCanonicalValue(true));
        }

        return null;
    }

    public static OptionalInt canonicalizationAwareHash(Node node)
    {
        if (node instanceof Identifier identifier) {
            return OptionalInt.of(identifier.getCanonicalValue().hashCode());
        }
        if (node.getChildren().isEmpty()) {
            return OptionalInt.of(node.hashCode());
        }
        return OptionalInt.empty();
    }

    public static OptionalInt canonicalizationAwareIgnoreCaseHash(Node node)
    {
        if (node instanceof Identifier identifier) {
            return OptionalInt.of(identifier.getCanonicalValue(true).hashCode());
        }
        if (node.getChildren().isEmpty()) {
            return OptionalInt.of(node.hashCode());
        }
        return OptionalInt.empty();
    }
}
