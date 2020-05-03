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

import com.google.common.io.Closer;
import org.jline.reader.Completer;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import static io.trino.cli.TerminalUtils.isRealTerminal;
import static org.jline.reader.LineReader.BLINK_MATCHING_PAREN;
import static org.jline.reader.LineReader.HISTORY_FILE;
import static org.jline.reader.LineReader.MAIN;
import static org.jline.reader.LineReader.Option.HISTORY_TIMESTAMPED;
import static org.jline.reader.LineReader.SECONDARY_PROMPT_PATTERN;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.DEFAULT;

public class InputReader
        implements Closeable
{
    private final LineReader reader;

    public InputReader(ClientOptions.EditingMode editingMode, Path historyFile, Completer... completers)
            throws IOException
    {
        reader = LineReaderBuilder.builder()
                .terminal(TerminalUtils.getTerminal())
                .variable(HISTORY_FILE, historyFile)
                .variable(SECONDARY_PROMPT_PATTERN, isRealTerminal() ? colored("%P -> ") : "") // workaround for https://github.com/jline/jline3/issues/751
                .variable(BLINK_MATCHING_PAREN, 0)
                .parser(new InputParser())
                .highlighter(new InputHighlighter())
                .completer(new AggregateCompleter(completers))
                .build();

        reader.getKeyMaps().put(MAIN, reader.getKeyMaps().get(editingMode.getKeyMap()));
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
