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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ThriftHttpMetastoreConfig
{
    public enum AuthenticationMode
    {
        BEARER
    }

    private Duration readTimeout = new Duration(60, TimeUnit.SECONDS);
    private AuthenticationMode authenticationMode;
    private Optional<String> httpBearerToken = Optional.empty();
    private Map<String, String> additionalHeaders = ImmutableMap.of();

    @NotNull
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("hive.metastore.http.client.read-timeout")
    @ConfigDescription("Socket read timeout for metastore client")
    public ThriftHttpMetastoreConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    @NotNull
    public Optional<AuthenticationMode> getAuthenticationMode()
    {
        return Optional.ofNullable(authenticationMode);
    }

    @Config("hive.metastore.http.client.authentication.type")
    @ConfigDescription("Authentication mode for thrift http based metastore client")
    public ThriftHttpMetastoreConfig setAuthenticationMode(AuthenticationMode authenticationMode)
    {
        this.authenticationMode = authenticationMode;
        return this;
    }

    @NotNull
    public Optional<String> getHttpBearerToken()
    {
        return httpBearerToken;
    }

    @Config("hive.metastore.http.client.bearer-token")
    @ConfigSecuritySensitive
    @ConfigDescription("Bearer token to authenticate with a HTTP transport based metastore service")
    public ThriftHttpMetastoreConfig setHttpBearerToken(String httpBearerToken)
    {
        this.httpBearerToken = Optional.ofNullable(httpBearerToken);
        return this;
    }

    public Map<String, String> getAdditionalHeaders()
    {
        return additionalHeaders;
    }

    @Config("hive.metastore.http.client.additional-headers")
    @ConfigDescription("Comma separated key:value pairs to be send to metastore as additional headers")
    public ThriftHttpMetastoreConfig setAdditionalHeaders(String httpHeaders)
    {
        try {
            // we allow escaping the delimiters like , and : using back-slash.
            // To support that we create a negative lookbehind of , and : which
            // are not preceded by a back-slash.
            String headersDelim = "(?<!\\\\),";
            String kvDelim = "(?<!\\\\):";
            Map<String, String> temp = new HashMap<>();
            if (httpHeaders != null) {
                for (String kv : httpHeaders.split(headersDelim)) {
                    String key = kv.split(kvDelim, 2)[0].trim();
                    String val = kv.split(kvDelim, 2)[1].trim();
                    temp.put(key, val);
                }
                this.additionalHeaders = ImmutableMap.copyOf(temp);
            }
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format("Invalid format for 'hive.metastore.http.client.additional-headers'. " +
                    "Value provided is %s", httpHeaders), e);
        }
        return this;
    }
}
