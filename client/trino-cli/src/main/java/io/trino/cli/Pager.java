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

import com.google.common.collect.ImmutableList;
import org.jline.builtins.Less;
import org.jline.builtins.Options;
import org.jline.builtins.Source;
import org.jline.terminal.Terminal;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;

public class Pager
        extends FilterOutputStream
{
    public static final String ENV_PAGER = "TRINO_PAGER";
    public static final String ENV_PAGER_BUILTIN = "TRINO_PAGER_BUILTING";
    /**
     * Options are:
     * -F or --quit-if-one-screen
     * -X or --no-init avoids clearing the screen after closing
     * -R or --RAW-CONTROL-CHARS
     * -S or --chop-long-lines
     * -n or --line-numbers (suppress)
     */
    public static final List<String> LESS = ImmutableList.of("less", "-FXRSn");

    private final Process process;
    private final Thread thread;

    private Pager(OutputStream out)
    {
        super(out);
        this.process = null;
        this.thread = null;
    }

    private Pager(OutputStream out, Process process)
    {
        super(out);
        this.process = process;
        this.thread = null;
    }

    private Pager(OutputStream out, Thread thread)
    {
        super(out);
        this.process = null;
        this.thread = thread;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            super.close();
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
        finally {
            if (process != null) {
                try {
                    process.waitFor();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    process.destroy();
                }
            }
            if (thread != null) {
                try {
                    thread.join();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public boolean isNullPager()
    {
        return process == null && thread == null;
    }

    @Override
    public void write(int b)
            throws IOException
    {
        try {
            super.write(b);
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        try {
            super.write(b, off, len);
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        try {
            super.flush();
        }
        catch (IOException e) {
            throw propagateIOException(e);
        }
    }

    public CompletableFuture<?> getFinishFuture()
    {
        checkState(process != null || thread != null, "getFinishFuture called on null pager");
        CompletableFuture<?> result = new CompletableFuture<>();
        new Thread(() -> {
            try {
                if (process != null) {
                    process.waitFor();
                }
                else {
                    thread.join();
                }
            }
            catch (InterruptedException e) {
                // ignore exception because thread is exiting
            }
            finally {
                result.complete(null);
            }
        }).start();
        return result;
    }

    private static IOException propagateIOException(IOException e)
            throws IOException
    {
        // TODO: check if the pager exited and verify the exit status?
        if ("Broken pipe".equals(e.getMessage()) || "Stream closed".equals(e.getMessage()) || "Pipe closed".equals(e.getMessage())) {
            throw new QueryAbortedException(e);
        }
        throw e;
    }

    public static Pager create(Terminal terminal)
    {
        String builtin = System.getenv(ENV_PAGER_BUILTIN);
        if (builtin != null && builtin.equals("true")) {
            return createBuiltinPager(terminal);
        }
        String pager = System.getenv(ENV_PAGER);
        if (pager == null) {
            return create(LESS);
        }
        pager = pager.trim();
        if (pager.isEmpty()) {
            return createNullPager();
        }
        return create(ImmutableList.of("/bin/sh", "-c", pager));
    }

    public static Pager create(List<String> command)
    {
        try {
            Process process = new ProcessBuilder()
                    .command(command)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();
            return new Pager(process.getOutputStream(), process);
        }
        catch (IOException e) {
            System.err.println("ERROR: failed to open pager: " + e.getMessage());
            return createNullPager();
        }
    }

    private static Pager createBuiltinPager(Terminal terminal)
    {
        Options opt = Options.compile(Less.usage()).parse(Arrays.asList("--quit-if-one-screen", "--no-init", "--chop-long-lines"));
        Less less = new Less(terminal, Paths.get("").toAbsolutePath(), opt);
        PipedInputStream in = new PipedInputStream();
        final PipedOutputStream out;
        try {
            out = new PipedOutputStream(in);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        // TODO it might not be the best idea to run it in a different thread, since it's using the same terminal
        // maybe try to wrap the client handler in a thread and block on less.run()
        Thread thread = new Thread(() -> {
            try {
                Source.InputStreamSource src = new Source.InputStreamSource(in, true, "query results");
                less.run(new LinkedList<>(Arrays.asList(src)));
                out.close();
                in.close();
            }
            catch (IOException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });
        thread.start();
        return new Pager(out, thread);
    }

    private static Pager createNullPager()
    {
        return new Pager(uncloseableOutputStream(System.out));
    }

    private static OutputStream uncloseableOutputStream(OutputStream out)
    {
        return new FilterOutputStream(out)
        {
            @Override
            public void close()
                    throws IOException
            {
                flush();
            }
        };
    }
}
