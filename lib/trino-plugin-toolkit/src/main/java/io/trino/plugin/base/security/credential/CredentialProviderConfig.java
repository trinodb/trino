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
package io.trino.plugin.base.security.credential;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class CredentialProviderConfig
{
    private String name;

    @Config("name")
    @ConfigDescription("The name of the properties file in the etc/credential-provider directory")
    public CredentialProviderConfig setName(String name)
    {
        this.name = name;
        return this;
    }

    @NotNull
    public String getName()
    {
        return name;
    }
}
