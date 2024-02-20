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
package io.trino.sql.testing;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;
import jakarta.annotation.Nullable;

import java.util.List;

import static io.trino.sql.SqlFormatter.formatSql;

public final class TreeAssertions
{
    private TreeAssertions() {}

    public static void assertFormattedSql(SqlParser sqlParser, Node expected)
    {
        String formatted = formatSql(expected);

        // verify round-trip of formatting already-formatted SQL
        Statement actual = parseFormatted(sqlParser, formatted, expected);
        assertEquals(formatSql(actual), formatted);

        // compare parsed tree with parsed tree of formatted SQL
        if (!actual.equals(expected)) {
            // simplify finding the non-equal part of the tree
            assertListEquals(linearizeTree(actual), linearizeTree(expected));
        }
        assertEquals(actual, expected);
    }

    private static Statement parseFormatted(SqlParser sqlParser, String sql, Node tree)
    {
        try {
            return sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            String message = "failed to parse formatted SQL: %s\nerror: %s\ntree: %s".formatted(sql, e.getMessage(), tree);
            throw new AssertionError(message, e);
        }
    }

    private static List<Node> linearizeTree(Node tree)
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            public Void process(Node node, @Nullable Void context)
            {
                super.process(node, context);
                nodes.add(node);
                return null;
            }
        }.process(tree, null);
        return nodes.build();
    }

    private static <T> void assertListEquals(List<T> actual, List<T> expected)
    {
        if (actual.size() != expected.size()) {
            throw new AssertionError("Lists not equal in size%n%s".formatted(formatLists(actual, expected)));
        }
        if (!actual.equals(expected)) {
            throw new AssertionError("Lists not equal at index %s%n%s".formatted(
                    differingIndex(actual, expected), formatLists(actual, expected)));
        }
    }

    private static <T> String formatLists(List<T> actual, List<T> expected)
    {
        Joiner joiner = Joiner.on("\n    ");
        return "Actual [%s]:%n    %s%nExpected [%s]:%n    %s%n".formatted(
                actual.size(), joiner.join(actual),
                expected.size(), joiner.join(expected));
    }

    private static <T> int differingIndex(List<T> actual, List<T> expected)
    {
        for (int i = 0; i < actual.size(); i++) {
            if (!actual.get(i).equals(expected.get(i))) {
                return i;
            }
        }
        return actual.size();
    }

    private static <T> void assertEquals(T actual, T expected)
    {
        if (!actual.equals(expected)) {
            throw new AssertionError("expected [%s] but found [%s]".formatted(expected, actual));
        }
    }
}
