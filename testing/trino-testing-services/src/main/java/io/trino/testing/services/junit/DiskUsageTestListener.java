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
package io.trino.testing.services.junit;

import io.airlift.log.Logger;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class DiskUsageTestListener
        implements TestExecutionListener
{
    private static final Logger log = Logger.get(DiskUsageTestListener.class);

    private final Map<String, Long> classDiskBefore = new HashMap<>();

    @Override
    public void executionStarted(TestIdentifier testIdentifier)
    {
        if (testIdentifier.isContainer() && testIdentifier.getSource().isPresent()) {
            String className = testIdentifier.getDisplayName();
            classDiskBefore.put(className, getDiskFree());
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult)
    {
        if (testIdentifier.isContainer() && testIdentifier.getSource().isPresent()) {
            String className = testIdentifier.getDisplayName();
            Long before = classDiskBefore.get(className);
            if (before != null) {
                long after = getDiskFree();
                long used = before - after;
                log.warn("Class %s used approximately %d bytes of disk%n", className, used);
            }
        }
    }

    private static long getDiskFree()
    {
        try {
            Path path = Paths.get(System.getProperty("java.io.tmpdir"));
            FileStore store = Files.getFileStore(path);
            return store.getUsableSpace();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
