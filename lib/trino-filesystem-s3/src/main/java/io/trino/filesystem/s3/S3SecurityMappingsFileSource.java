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

import com.google.inject.Inject;

import java.io.File;
import java.util.function.Supplier;

import static io.trino.plugin.base.util.JsonUtils.parseJson;

class S3SecurityMappingsFileSource
        implements Supplier<S3SecurityMappings>
{
    private final File configFile;
    private final String jsonPointer;

    @Inject
    public S3SecurityMappingsFileSource(S3SecurityMappingConfig config)
    {
        this.configFile = config.getConfigFile().orElseThrow();
        this.jsonPointer = config.getJsonPointer();
    }

    @Override
    public S3SecurityMappings get()
    {
        return parseJson(configFile.toPath(), jsonPointer, S3SecurityMappings.class);
    }
}
