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
package io.trino.client.auth.external;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ref.Reference;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Takes an {@link InterProcessLock} in a separate JVM, as another client process would.
 */
final class TestingLockHolder
{
    private TestingLockHolder() {}

    /**
     * Starts a process that tries to take the lock on {@code file} and, if it succeeds, holds it until it is killed.
     */
    public static Process startLockHolder(Path file)
            throws IOException
    {
        return start("hold", file);
    }

    /**
     * Whether the process started by {@link #startLockHolder} took the lock.
     */
    public static boolean acquiredLock(Process process)
            throws IOException
    {
        String line = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8)).readLine();
        checkState("acquired".equals(line) || "busy".equals(line), "Unexpected output: %s", line);
        return "acquired".equals(line);
    }

    /**
     * Starts a process that holds the lock on {@code file} until it is killed.
     */
    public static Process holdInAnotherProcess(Path file)
            throws IOException
    {
        return hold("hold", file);
    }

    /**
     * Starts a process that holds a shared lock on {@code file} until it is killed,
     * as {@link InterProcessLock#isLocked} does for a moment.
     */
    public static Process holdSharedInAnotherProcess(Path file)
            throws IOException
    {
        return hold("share", file);
    }

    private static Process hold(String mode, Path file)
            throws IOException
    {
        Process process = start(mode, file);
        try {
            checkState(acquiredLock(process), "Failed to lock %s in another process", file);
            return process;
        }
        catch (RuntimeException | IOException e) {
            process.destroyForcibly();
            throw e;
        }
    }

    /**
     * Whether another process can take the lock on {@code file}.
     */
    public static boolean tryLockInAnotherProcess(Path file)
            throws IOException, InterruptedException
    {
        Process process = start("try", file);
        try {
            return acquiredLock(process);
        }
        finally {
            process.destroyForcibly().waitFor();
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException
    {
        Path file = Path.of(args[1]);
        if (args[0].equals("share")) {
            try (FileChannel channel = FileChannel.open(file, CREATE, READ, WRITE)) {
                System.out.println(channel.tryLock(0, Long.MAX_VALUE, true) != null ? "acquired" : "busy");
                System.out.flush();
                Thread.sleep(Long.MAX_VALUE);
            }
            return;
        }
        InterProcessLock lock = new InterProcessLock(file);
        boolean acquired = lock.tryLock();
        System.out.println(acquired ? "acquired" : "busy");
        System.out.flush();
        if (acquired && args[0].equals("hold")) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            }
            finally {
                // The lock is released when the instance is garbage collected
                Reference.reachabilityFence(lock);
            }
        }
    }

    private static Process start(String mode, Path file)
            throws IOException
    {
        return new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                TestingLockHolder.class.getName(),
                mode,
                file.toString())
                .redirectError(INHERIT)
                .start();
    }
}
