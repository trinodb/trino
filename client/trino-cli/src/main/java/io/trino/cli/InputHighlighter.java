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
package io.trino.cli;

import io.trino.cli.lexer.StatementSplitter;
import io.trino.grammar.sql.SqlBaseLexer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;

import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cli.Console.STATEMENT_DELIMITERS;
import static io.trino.grammar.sql.SqlKeywords.sqlKeywords;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class InputHighlighter
        implements Highlighter
{
    private static final Set<String> KEYWORDS = sqlKeywords().stream()
            .map(keyword -> keyword.toLowerCase(ENGLISH))
            .collect(toImmutableSet());

    private final Theme theme;

    public InputHighlighter(Theme theme)
    {
        this.theme = requireNonNull(theme, "theme is null");
    }

    @Override
    public AttributedString highlight(LineReader reader, String buffer)
    {
        TokenSource tokens = StatementSplitter.getLexer(buffer, STATEMENT_DELIMITERS);
        AttributedStringBuilder builder = new AttributedStringBuilder();

        boolean error = false;
        while (true) {
            Token token = tokens.nextToken();
            int type = token.getType();
            if (type == Token.EOF) {
                break;
            }
            String text = token.getText();

            if (error || (type == SqlBaseLexer.UNRECOGNIZED)) {
                error = true;
                builder.styled(theme.error(), text);
            }
            else if (isKeyword(text)) {
                builder.styled(theme.keyword(), text);
            }
            else if (isString(type)) {
                builder.styled(theme.string(), text);
            }
            else if (isNumber(type)) {
                builder.styled(theme.number(), text);
            }
            else if (isComment(type)) {
                builder.styled(theme.comment(), text);
            }
            else {
                builder.append(text);
            }
        }

        return builder.toAttributedString();
    }

    @Override
    public void setErrorPattern(Pattern pattern) {}

    @Override
    public void setErrorIndex(int i) {}

    private static boolean isKeyword(String text)
    {
        return KEYWORDS.contains(text.toLowerCase(ENGLISH));
    }

    private static boolean isString(int type)
    {
        return (type == SqlBaseLexer.STRING) ||
                (type == SqlBaseLexer.UNICODE_STRING) ||
                (type == SqlBaseLexer.BINARY_LITERAL);
    }

    private static boolean isNumber(int type)
    {
        return (type == SqlBaseLexer.INTEGER_VALUE) ||
                (type == SqlBaseLexer.DOUBLE_VALUE) ||
                (type == SqlBaseLexer.DECIMAL_VALUE);
    }

    private static boolean isComment(int type)
    {
        return (type == SqlBaseLexer.SIMPLE_COMMENT) ||
                (type == SqlBaseLexer.BRACKETED_COMMENT);
    }
}
