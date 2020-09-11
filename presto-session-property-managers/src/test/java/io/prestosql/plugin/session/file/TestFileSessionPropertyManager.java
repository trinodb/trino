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

package io.prestosql.plugin.session.file;

import io.airlift.testing.TempFile;
import io.prestosql.plugin.session.AbstractTestSessionPropertyManager;
import io.prestosql.plugin.session.SessionMatchSpec;
import io.prestosql.spi.session.SessionPropertyConfigurationManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static io.prestosql.plugin.session.file.FileSessionPropertyManager.CODEC;
import static org.testng.Assert.assertEquals;

public class TestFileSessionPropertyManager
        extends AbstractTestSessionPropertyManager
{
    @Override
    protected void assertProperties(Map<String, String> properties, SessionMatchSpec... spec)
            throws IOException
    {
        try (TempFile tempFile = new TempFile()) {
            Path configurationFile = tempFile.path();
            Files.write(configurationFile, CODEC.toJsonBytes(Arrays.asList(spec)));
            SessionPropertyConfigurationManager manager = new FileSessionPropertyManager(new FileSessionPropertyManagerConfig().setConfigFile(configurationFile.toFile()));
            assertEquals(manager.getSystemSessionProperties(CONTEXT), properties);
        }
    }
}
