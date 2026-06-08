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

import org.jline.utils.AttributedStyle;

import static java.util.Objects.requireNonNull;
import static org.jline.utils.AttributedStyle.BOLD;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.GREEN;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

/**
 * The set of styles applied to the colored elements of the CLI. A single theme
 * is resolved once at startup and shared by every component that emits ANSI
 * styling, so that the look of the CLI can be changed in one place.
 */
public final class Theme
{
    public static final Theme DARK = new Theme(
            BOLD,
            DEFAULT.foreground(GREEN),
            DEFAULT.foreground(CYAN),
            DEFAULT.foreground(BRIGHT).italic(),
            DEFAULT.foreground(RED),
            DEFAULT.foreground(YELLOW),
            DEFAULT.foreground(BRIGHT),
            DEFAULT.foreground(CYAN),
            DEFAULT.foreground(CYAN),
            BOLD.foreground(RED));

    private final AttributedStyle keyword;
    private final AttributedStyle string;
    private final AttributedStyle number;
    private final AttributedStyle comment;
    private final AttributedStyle error;
    private final AttributedStyle warning;
    private final AttributedStyle prompt;
    private final AttributedStyle historyIndex;
    private final AttributedStyle errorContext;
    private final AttributedStyle cliError;

    public Theme(
            AttributedStyle keyword,
            AttributedStyle string,
            AttributedStyle number,
            AttributedStyle comment,
            AttributedStyle error,
            AttributedStyle warning,
            AttributedStyle prompt,
            AttributedStyle historyIndex,
            AttributedStyle errorContext,
            AttributedStyle cliError)
    {
        this.keyword = requireNonNull(keyword, "keyword is null");
        this.string = requireNonNull(string, "string is null");
        this.number = requireNonNull(number, "number is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.error = requireNonNull(error, "error is null");
        this.warning = requireNonNull(warning, "warning is null");
        this.prompt = requireNonNull(prompt, "prompt is null");
        this.historyIndex = requireNonNull(historyIndex, "historyIndex is null");
        this.errorContext = requireNonNull(errorContext, "errorContext is null");
        this.cliError = requireNonNull(cliError, "cliError is null");
    }

    public AttributedStyle keyword()
    {
        return keyword;
    }

    public AttributedStyle string()
    {
        return string;
    }

    public AttributedStyle number()
    {
        return number;
    }

    public AttributedStyle comment()
    {
        return comment;
    }

    public AttributedStyle error()
    {
        return error;
    }

    public AttributedStyle warning()
    {
        return warning;
    }

    public AttributedStyle prompt()
    {
        return prompt;
    }

    public AttributedStyle historyIndex()
    {
        return historyIndex;
    }

    public AttributedStyle errorContext()
    {
        return errorContext;
    }

    public AttributedStyle cliError()
    {
        return cliError;
    }
}
