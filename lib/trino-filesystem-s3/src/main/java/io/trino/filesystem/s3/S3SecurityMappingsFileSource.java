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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;

class S3SecurityMappingsFileSource
        implements Supplier<S3SecurityMappings>
{
    private final Path configFile;
    private final String jsonPointer;
    private final SecretsResolver secretsResolver;

    @Inject
    public S3SecurityMappingsFileSource(S3SecurityMappingConfig config, SecretsResolver secretsResolver)
    {
        this.configFile = config.getConfigFile().orElseThrow().toPath();
        this.jsonPointer = config.getJsonPointer();
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    @Override
    public S3SecurityMappings get()
    {
        try {
            String json = Files.readString(configFile);
            String resolved = secretsResolver.getResolvedConfiguration(ImmutableMap.of("json", json)).get("json");
            return parseJson(resolved, jsonPointer, S3SecurityMappings.class);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read security mapping file: " + configFile, e);
        }
    }
}
