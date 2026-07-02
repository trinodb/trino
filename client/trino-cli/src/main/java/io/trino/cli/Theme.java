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
import static org.jline.utils.AttributedStyle.BLUE;
import static org.jline.utils.AttributedStyle.BOLD;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.GREEN;
import static org.jline.utils.AttributedStyle.MAGENTA;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

/**
 * The set of styles applied to the colored elements of the CLI. A single theme
 * is resolved once at startup and shared by every component that emits ANSI
 * styling, so that the look of the CLI can be changed in one place.
 * <p>
 * Each constructor argument is a role, in order: keyword, string, number,
 * comment, error, warning, prompt, historyIndex, errorContext, cliError. The
 * named schemes use their authors' exact (24-bit) palettes; JLine downsamples
 * these to the terminal's color capability, so they degrade gracefully without
 * truecolor support.
 */
public enum Theme
{
    /**
     * Resolved at startup to a concrete theme: no color for a dumb terminal or
     * when {@code NO_COLOR} is set, otherwise {@link #DARK}. Carries no styling
     * of its own, since it is always resolved before use.
     */
    AUTO,

    /**
     * Tuned for a dark background. Reproduces the styling the CLI used before themes were configurable.
     */
    DARK(BOLD,
            DEFAULT.foreground(GREEN),
            DEFAULT.foreground(CYAN),
            DEFAULT.foreground(BRIGHT).italic(),
            DEFAULT.foreground(RED),
            DEFAULT.foreground(YELLOW),
            DEFAULT.foreground(BRIGHT),
            DEFAULT.foreground(CYAN),
            DEFAULT.foreground(CYAN),
            BOLD.foreground(RED)),

    /**
     * Tuned for a light background, avoiding the high-intensity and yellow/cyan tones that wash out on white.
     */
    LIGHT(BOLD,
            DEFAULT.foreground(GREEN),
            DEFAULT.foreground(BLUE),
            DEFAULT.faint().italic(),
            DEFAULT.foreground(RED),
            DEFAULT.foreground(MAGENTA),
            DEFAULT.foreground(BLUE),
            DEFAULT.foreground(BLUE),
            DEFAULT.foreground(BLUE),
            BOLD.foreground(RED)),

    /**
     * No styling. Every role maps to the default style, so nothing emits ANSI escape codes.
     */
    NONE,

    /**
     * <a href="https://ethanschoonover.com/solarized/">Solarized</a>, tuned for a dark background.
     */
    SOLARIZED_DARK(
            rgb(0x859900).bold(),   // green
            rgb(0x2AA198),          // cyan
            rgb(0xD33682),          // magenta
            rgb(0x586E75).italic(), // base01
            rgb(0xDC322F),          // red
            rgb(0xB58900),          // yellow
            rgb(0x268BD2),          // blue
            rgb(0x6C71C4),          // violet
            rgb(0x93A1A1),          // base1
            rgb(0xDC322F).bold()),  // red

    /**
     * <a href="https://ethanschoonover.com/solarized/">Solarized</a>, tuned for a light background.
     */
    SOLARIZED_LIGHT(
            rgb(0x859900).bold(),   // green
            rgb(0x2AA198),          // cyan
            rgb(0xD33682),          // magenta
            rgb(0x93A1A1).italic(), // base1
            rgb(0xDC322F),          // red
            rgb(0xB58900),          // yellow
            rgb(0x268BD2),          // blue
            rgb(0x6C71C4),          // violet
            rgb(0x586E75),          // base01
            rgb(0xDC322F).bold()),  // red

    /**
     * <a href="https://draculatheme.com/">Dracula</a>.
     */
    DRACULA(rgb(0xFF79C6).bold(),   // pink
            rgb(0xF1FA8C),          // yellow
            rgb(0xBD93F9),          // purple
            rgb(0x6272A4).italic(), // comment
            rgb(0xFF5555),          // red
            rgb(0xFFB86C),          // orange
            rgb(0xBD93F9),          // purple
            rgb(0x50FA7B),          // green
            rgb(0x8BE9FD),          // cyan
            rgb(0xFF5555).bold()),  // red

    /**
     * <a href="https://www.nordtheme.com/">Nord</a>.
     */
    NORD(rgb(0x81A1C1).bold(),   // nord9
            rgb(0xA3BE8C),          // nord14
            rgb(0xB48EAD),          // nord15
            rgb(0x4C566A).italic(), // nord3
            rgb(0xBF616A),          // nord11
            rgb(0xEBCB8B),          // nord13
            rgb(0x88C0D0),          // nord8
            rgb(0x8FBCBB),          // nord7
            rgb(0x81A1C1),          // nord9
            rgb(0xBF616A).bold()),  // nord11

    /**
     * <a href="https://github.com/morhetz/gruvbox">Gruvbox</a>, tuned for a dark background.
     */
    GRUVBOX_DARK(
            rgb(0xFB4934).bold(),   // red
            rgb(0xB8BB26),          // green
            rgb(0xD3869B),          // purple
            rgb(0x928374).italic(), // gray
            rgb(0xFB4934),          // red
            rgb(0xFABD2F),          // yellow
            rgb(0x83A598),          // blue
            rgb(0x8EC07C),          // aqua
            rgb(0x83A598),          // blue
            rgb(0xFB4934).bold());  // red

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

    Theme()
    {
        this(DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT);
    }

    Theme(AttributedStyle keyword,
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

    /**
     * Resolve this selection to a concrete theme. Every theme except
     * {@link #AUTO} resolves to itself; {@code AUTO} disables color for a dumb
     * terminal or when {@code NO_COLOR} is set, and otherwise falls back to
     * {@link #DARK}.
     */
    public Theme resolve(boolean realTerminal, boolean noColor)
    {
        if (this != AUTO) {
            return this;
        }
        if (noColor || !realTerminal) {
            return NONE;
        }
        return DARK;
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

    private static AttributedStyle rgb(int color)
    {
        return DEFAULT.foregroundRgb(color);
    }
}
