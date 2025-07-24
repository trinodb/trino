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
package io.trino.iam.aws;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;

import java.io.File;
import java.util.function.Supplier;

import static io.trino.plugin.base.util.JsonUtils.parseJson;

public class IAMSecurityMappingsFileSource<T extends IAMSecurityMappings<E>, E extends IAMSecurityMapping, C extends IAMSecurityMappingConfig>
        implements Supplier<T>
{
    private final File configFile;
    private final String jsonPointer;
    private final TypeReference<T> mappingsTypeReference;

    @Inject
    public IAMSecurityMappingsFileSource(C config, TypeReference<T> mappingsTypeReference)
    {
        this.configFile = config.getConfigFile().orElseThrow();
        this.jsonPointer = config.getJsonPointer();
        this.mappingsTypeReference = mappingsTypeReference;
    }

    @Override
    public T get()
    {
        return parseJson(configFile.toPath(), jsonPointer, mappingsTypeReference);
    }
}
