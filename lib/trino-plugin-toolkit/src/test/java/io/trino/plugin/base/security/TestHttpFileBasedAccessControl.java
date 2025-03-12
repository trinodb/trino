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
import io.trino.spi.connector.ConnectorAccessControl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAll;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHttpFileBasedAccessControl
        extends BaseFileBasedConnectorAccessControlTest
{
    private final TestingHttpServer testingHttpServer = new TestingHttpServer();

    @AfterAll
    public void tearDown()
            throws IOException
    {
        closeAll(testingHttpServer);
    }

    @Override
    protected ConnectorAccessControl createAccessControl(Path configFile, Map<String, String> properties)
    {
        String dataUrl = testingHttpServer.resource(configFile.normalize().toAbsolutePath().toString()).toString();
        return createAccessControl(ImmutableMap.<String, String>builder().putAll(properties).put("security.config-file",
                        dataUrl).buildOrThrow());
    }
}
