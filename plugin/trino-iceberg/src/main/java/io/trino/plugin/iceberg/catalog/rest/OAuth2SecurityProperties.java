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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class OAuth2SecurityProperties
        implements SecurityProperties
{
    private final Map<String, String> securityProperties;

    @Inject
    public OAuth2SecurityProperties(OAuth2SecurityConfig securityConfig)
    {
        requireNonNull(securityConfig, "securityConfig is null");

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        securityConfig.getCredential().ifPresent(
                value -> propertiesBuilder.put(OAuth2Properties.CREDENTIAL, value));
        securityConfig.getToken().ifPresent(
                value -> propertiesBuilder.put(OAuth2Properties.TOKEN, value));

        this.securityProperties = propertiesBuilder.buildOrThrow();
    }

    @Override
    public Map<String, String> get()
    {
        return securityProperties;
    }
}
