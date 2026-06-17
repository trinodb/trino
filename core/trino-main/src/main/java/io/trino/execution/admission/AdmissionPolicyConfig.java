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
package io.trino.execution.admission;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class AdmissionPolicyConfig
{
    public static final String DEFAULT_NAME = "min-workers";

    private String name = DEFAULT_NAME;

    @NotNull
    public String getName()
    {
        return name;
    }

    @Config("query-manager.admission-policy.name")
    @ConfigDescription("Stable factory name of the bound AdmissionPolicy")
    public AdmissionPolicyConfig setName(String name)
    {
        this.name = name;
        return this;
    }
}
