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
package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.TestingHttpServer;
import io.trino.spi.security.SystemAccessControl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAll;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHttpFileBasedSystemAccessControl
        extends BaseFileBasedSystemAccessControlTest
{
    private final TestingHttpServer testingHttpServer = new TestingHttpServer();

    @AfterAll
    public void tearDown()
            throws IOException
    {
        closeAll(testingHttpServer);
    }

    @Override
    protected SystemAccessControl newFileBasedSystemAccessControl(File configFile, Map<String, String> properties)
    {
        try {
            String dataUrl = testingHttpServer.resource(configFile.getCanonicalFile().getAbsolutePath()).toString();
            return newFileBasedSystemAccessControl(ImmutableMap.<String, String>builder().putAll(properties).put("security.config-file", dataUrl).buildOrThrow());
        }
        catch (IOException e) {
            throw new RuntimeException("Error while creating SystemAccessControl", e);
        }
    }
}
