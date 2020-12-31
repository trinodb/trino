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

import java.io.PrintStream;

import static io.trino.cli.TerminalUtils.isRealTerminal;
import static java.util.Objects.requireNonNull;

public class ConsolePrinter
{
    private static final String ERASE_SCREEN_FORWARD = "\033[0J";
    private static final String ERASE_LINE_ALL = "\033[2K";

    private final PrintStream out;
    private int lines;

    public ConsolePrinter(PrintStream out)
    {
        this.out = requireNonNull(out, "out is null");
    }

    public void reprintLine(String line)
    {
        if (isRealTerminal()) {
            out.print(ERASE_LINE_ALL + line + "\n");
        }
        else {
            out.print('\r' + line);
        }
        out.flush();
        lines++;
    }

    public void repositionCursor()
    {
        if (lines > 0) {
            if (isRealTerminal()) {
                out.print(cursorUp(lines));
            }
            else {
                out.print('\r');
            }
            out.flush();
            lines = 0;
        }
    }

    public void resetScreen()
    {
        if (lines > 0) {
            if (isRealTerminal()) {
                out.print(cursorUp(lines) + ERASE_SCREEN_FORWARD);
            }
            else {
                out.print('\r');
            }
            out.flush();
            lines = 0;
        }
    }

    private static String cursorUp(int lines)
    {
        return "\033[" + lines + "A";
    }
}
