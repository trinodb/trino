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
package io.prestosql.cli;

import io.prestosql.sql.parser.SqlBaseLexer;
import io.prestosql.sql.parser.StatementSplitter;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.cli.Console.STATEMENT_DELIMITERS;
import static io.prestosql.sql.ReservedIdentifiers.sqlKeywords;
import static java.util.Locale.ENGLISH;
import static org.jline.utils.AttributedStyle.BOLD;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.GREEN;
import static org.jline.utils.AttributedStyle.RED;

public class InputHighlighter
        implements Highlighter
{
    private static final AttributedStyle KEYWORD_STYLE = BOLD;
    private static final AttributedStyle STRING_STYLE = DEFAULT.foreground(GREEN);
    private static final AttributedStyle NUMBER_STYLE = DEFAULT.foreground(CYAN);
    private static final AttributedStyle COMMENT_STYLE = DEFAULT.foreground(BRIGHT).italic();
    private static final AttributedStyle ERROR_STYLE = DEFAULT.foreground(RED);

    private static final Set<String> KEYWORDS = sqlKeywords().stream()
            .map(keyword -> keyword.toLowerCase(ENGLISH))
            .collect(toImmutableSet());

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
                builder.styled(ERROR_STYLE, text);
            }
            else if (isKeyword(text)) {
                builder.styled(KEYWORD_STYLE, text);
            }
            else if (isString(type)) {
                builder.styled(STRING_STYLE, text);
            }
            else if (isNumber(type)) {
                builder.styled(NUMBER_STYLE, text);
            }
            else if (isComment(type)) {
                builder.styled(COMMENT_STYLE, text);
            }
            else {
                builder.append(text);
            }
        }

        return builder.toAttributedString();
    }

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
