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

import com.google.common.io.Closer;
import org.jline.reader.Completer;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import static org.jline.reader.LineReader.BLINK_MATCHING_PAREN;
import static org.jline.reader.LineReader.HISTORY_FILE;
import static org.jline.reader.LineReader.Option.HISTORY_TIMESTAMPED;
import static org.jline.reader.LineReader.SECONDARY_PROMPT_PATTERN;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.DEFAULT;

public class InputReader
        implements Closeable
{
    private final LineReader reader;

    public InputReader(Path historyFile, Completer... completers)
            throws IOException
    {
        Terminal terminal = TerminalBuilder.builder()
                .name("Presto")
                .build();

        reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .variable(HISTORY_FILE, historyFile)
                .variable(SECONDARY_PROMPT_PATTERN, colored("%P -> "))
                .variable(BLINK_MATCHING_PAREN, 0)
                .parser(new InputParser())
                .highlighter(new InputHighlighter())
                .completer(new AggregateCompleter(completers))
                .build();

        reader.unsetOpt(HISTORY_TIMESTAMPED);
    }

    public String readLine(String prompt, String buffer)
    {
        return reader.readLine(colored(prompt), null, buffer);
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(getHistory()::save);
            closer.register(getTerminal());
        }
    }

    public History getHistory()
    {
        return reader.getHistory();
    }

    public Terminal getTerminal()
    {
        return reader.getTerminal();
    }

    private static String colored(String value)
    {
        return new AttributedString(value, DEFAULT.foreground(BRIGHT)).toAnsi();
    }
}
