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
package io.trino.plugin.hive.s3;

import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import static java.lang.String.format;

public class FileBasedS3SecurityMappingsProvider
        implements S3SecurityMappingsProvider
{
    private static final Logger log = Logger.get(FileBasedS3SecurityMappingsProvider.class);
    private final File configFile;
    private final S3SecurityMappingsParser parser;

    @Inject
    public FileBasedS3SecurityMappingsProvider(S3SecurityMappingConfig config)
    {
        this.configFile = config.getConfigFilePath().map(File::new).orElseThrow(() -> new IllegalArgumentException("configFile not set"));
        if (!configFile.exists()) {
            throw new IllegalArgumentException(format("configFile %s does not exist", configFile.getAbsoluteFile()));
        }
        this.parser = new S3SecurityMappingsParser(config);
    }

    private String getRawJsonString()
    {
        log.info("Retrieving config from file %s", configFile);
        try {
            return Files.readString(configFile.toPath());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read file: " + configFile, e);
        }
    }

    @Override
    public S3SecurityMappings get()
    {
        return parser.parseJSONString(getRawJsonString());
    }
}
