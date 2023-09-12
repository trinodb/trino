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
package io.trino.plugin.pinot.auth.password.inline;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class PinotPasswordControllerAuthenticationConfig
{
    private String user;
    private String password;

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("pinot.controller.authentication.user")
    public PinotPasswordControllerAuthenticationConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("pinot.controller.authentication.password")
    @ConfigSecuritySensitive
    public PinotPasswordControllerAuthenticationConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
