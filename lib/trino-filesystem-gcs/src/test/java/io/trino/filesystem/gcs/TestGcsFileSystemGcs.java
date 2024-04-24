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
package io.trino.filesystem.gcs;

import io.trino.filesystem.TrinoOutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGcsFileSystemGcs
        extends AbstractTestGcsFileSystem
{
    @BeforeAll
    void setup()
            throws IOException
    {
        initialize(getRequiredEnvironmentVariable("GCP_CREDENTIALS_KEY"));
    }

    @Test
    void testCreateFileRetry()
            throws Exception
    {
        // Note: this test is meant to expose flakiness
        // Without retries it may fail non-deterministically.
        // Retries are enabled in the default GcsFileSystemConfig.
        // In practice this may happen between 7 and 20 retries.
        for (int i = 1; i <= 30; i++) {
            TrinoOutputFile outputFile = getFileSystem().newOutputFile(getRootLocation().appendPath("testFile"));
            outputFile.createOrOverwrite("test".getBytes(UTF_8));
        }
    }
}
