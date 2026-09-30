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
package io.trino.plugin.ydb;

import java.util.Optional;

// Only the two-identifier equality emitted by convertPredicate is accepted.
// The owning relations and native types are recovered before QueryBuilder renders YQL.
record YdbJoinCondition(String left, String right)
{
    static Optional<YdbJoinCondition> parse(String expression)
    {
        Optional<Identifier> left = identifier(expression, whitespace(expression, 0));
        if (left.isEmpty()) {
            return Optional.empty();
        }
        int operator = whitespace(expression, left.get().end());
        if (operator == expression.length() || expression.charAt(operator) != '=') {
            return Optional.empty();
        }
        Optional<Identifier> right = identifier(expression, whitespace(expression, operator + 1));
        if (right.isEmpty() || whitespace(expression, right.get().end()) != expression.length()) {
            return Optional.empty();
        }
        return Optional.of(new YdbJoinCondition(left.get().name(), right.get().name()));
    }

    private static int whitespace(String expression, int offset)
    {
        while (offset < expression.length() && Character.isWhitespace(expression.charAt(offset))) {
            offset++;
        }
        return offset;
    }

    private static Optional<Identifier> identifier(String expression, int offset)
    {
        if (offset == expression.length() || expression.charAt(offset) != '`') {
            return Optional.empty();
        }
        StringBuilder name = new StringBuilder();
        for (int index = offset + 1; index < expression.length(); index++) {
            char character = expression.charAt(index);
            if (character == '`') {
                if (index + 1 < expression.length() && expression.charAt(index + 1) == '`') {
                    name.append('`');
                    index++;
                }
                else {
                    return Optional.of(new Identifier(name.toString(), index + 1));
                }
            }
            else {
                name.append(character);
            }
        }
        return Optional.empty();
    }

    private record Identifier(String name, int end) {}
}
