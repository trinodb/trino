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

import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.function.Supplier;

import static io.trino.plugin.base.security.FileBasedAccessControlUtils.parseJSONString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LocalFileAccessControlRulesProvider<R>
        extends RulesProvider<R>
{
    private static final Logger log = Logger.get(LocalFileAccessControlRulesProvider.class);
    private final File configFile;

    public LocalFileAccessControlRulesProvider(FileBasedAccessControlConfig config, Class<R> clazz)
    {
        super(config, clazz);
        this.configFile = new File(config.getConfigFilePath());
        if (!Files.isReadable(configFile.toPath())) {
            throw new IllegalArgumentException(format("configFile %s does not exist or is not readable",
                    configFile.getAbsoluteFile()));
        }
    }

    @Override
    protected String getRawJsonString()
    {
        log.info("Retrieving config from file %s", configFile);
        try {
            return Files.readString(configFile.toPath());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read file: " + configFile, e);
        }
    }
}
