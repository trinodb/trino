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
import io.airlift.configuration.DefunctConfig;
import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

@DefunctConfig({
        "hive.metastore.http.client.authentication.type",
        "hive.metastore.http.client.read-timeout",
})
public class ThriftHttpMetastoreConfig
{
    private Optional<String> bearerToken = Optional.empty();
    private Map<String, String> additionalHeaders = ImmutableMap.of();

    @NotNull
    public Optional<String> getBearerToken()
    {
        return bearerToken;
    }

    @Config("hive.metastore.http.client.bearer-token")
    @ConfigSecuritySensitive
    @ConfigDescription("Bearer token sent in the Authorization header to an https:// metastore")
    public ThriftHttpMetastoreConfig setBearerToken(String bearerToken)
    {
        this.bearerToken = Optional.ofNullable(bearerToken);
        return this;
    }

    @NotNull
    public Map<String, String> getAdditionalHeaders()
    {
        return additionalHeaders;
    }

    @Config("hive.metastore.http.client.additional-headers")
    @ConfigSecuritySensitive
    @ConfigDescription("Comma-separated name:value pairs sent as HTTP headers to the metastore")
    public ThriftHttpMetastoreConfig setAdditionalHeaders(String headers)
    {
        if (headers == null) {
            this.additionalHeaders = ImmutableMap.of();
            return this;
        }
        // ',' and ':' can be escaped with a backslash; split only on unescaped delimiters.
        // Header values may contain credentials, so they are not included in error messages.
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (String header : headers.split("(?<!\\\\),")) {
            String[] nameAndValue = header.split("(?<!\\\\):", 2);
            checkArgument(nameAndValue.length == 2, "Invalid format for 'hive.metastore.http.client.additional-headers', expected comma-separated name:value pairs");
            String name = unescape(nameAndValue[0].trim());
            checkArgument(!name.isEmpty(), "Header name must not be empty in 'hive.metastore.http.client.additional-headers'");
            builder.put(name, unescape(nameAndValue[1].trim()));
        }
        this.additionalHeaders = builder.buildOrThrow();
        return this;
    }

    private static String unescape(String value)
    {
        return value.replace("\\:", ":").replace("\\,", ",");
    }
}
