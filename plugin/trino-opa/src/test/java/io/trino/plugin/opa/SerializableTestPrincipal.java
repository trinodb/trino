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
package io.trino.plugin.opa;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.security.Principal;

// A Principal for tests that can be serialized using Jackson
@JsonSerialize
public class SerializableTestPrincipal
        implements Principal
{
    private String name;

    public SerializableTestPrincipal(String name)
    {
        this.name = name;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }
}
