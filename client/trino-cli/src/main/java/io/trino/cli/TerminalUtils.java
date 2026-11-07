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

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.terminal.spi.SystemStream;
import org.jline.terminal.spi.TerminalProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

public class TerminalUtils
{
    private static final Terminal TERMINAL_INSTANCE = createTerminal();
    private static final AtomicBoolean closed = new AtomicBoolean(false);

    private TerminalUtils() {}

    public static Terminal getTerminal()
    {
        checkState(!closed.get(), "Terminal is already closed");
        return TERMINAL_INSTANCE;
    }

    private static Terminal createTerminal()
    {
        try {
            try {
                return TerminalBuilder.builder()
                        .system(true)
                        .dumb(false)
                        .name("Trino")
                        .build();
            }
            catch (IllegalStateException e) {
                // Couldn't create a real terminal so we need to fallback to a dumb one
                return TerminalBuilder.builder()
                        .system(true)
                        .dumb(true)
                        .name("Trino")
                        .build();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean isRealTerminal(Terminal terminal)
    {
        return !Terminal.TYPE_DUMB.equals(terminal.getType()) &&
                !Terminal.TYPE_DUMB_COLOR.equals(terminal.getType());
    }

    public static boolean isRealTerminal()
    {
        return isRealTerminal(getTerminal());
    }

    /**
     * Whether standard output may be redirected to a regular file. This is a best effort check
     * that relies on the path of standard output. It is also true where that path does not exist,
     * as on Windows, because a file cannot be ruled out there.
     */
    public static boolean mayStandardOutputBeFile()
    {
        try {
            return mayBeFile(Path.of("/dev/stdout"));
        }
        catch (RuntimeException e) {
            return true;
        }
    }

    static boolean mayBeFile(Path standardOutput)
    {
        return !Files.exists(standardOutput) || Files.isRegularFile(standardOutput);
    }

    /**
     * Whether standard output is a terminal, regardless of standard input and standard error.
     */
    public static boolean isStandardOutputTerminal()
    {
        for (String name : List.of("ffm", "jni", "exec")) {
            try {
                return TerminalProvider.load(name).isSystemStream(SystemStream.Output);
            }
            catch (IOException | RuntimeException | LinkageError e) {
                // the provider is not available, try the next one
            }
        }
        return isRealTerminal();
    }

    public static int terminalWidth()
    {
        return getTerminal().getWidth();
    }

    public static Charset terminalEncoding()
    {
        return getTerminal().encoding();
    }

    public static void closeTerminal()
    {
        Terminal terminal = getTerminal();

        try {
            if (closed.compareAndSet(false, true)) {
                terminal.close();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
