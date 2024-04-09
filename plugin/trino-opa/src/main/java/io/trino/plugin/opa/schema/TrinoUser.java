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
package io.trino.plugin.opa.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.trino.spi.security.Identity;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

@JsonInclude(NON_NULL)
public record TrinoUser(String user, @JsonUnwrapped TrinoIdentity identity)
{
    public TrinoUser
    {
        if (identity == null) {
            requireNonNull(user, "user is null");
        }
        if (user != null && identity != null) {
            throw new IllegalArgumentException("user and identity may not both be set");
        }
    }

    public TrinoUser(String name)
    {
        this(name, null);
    }

    public TrinoUser(Identity identity)
    {
        this(null, TrinoIdentity.fromTrinoIdentity(identity));
    }
}
