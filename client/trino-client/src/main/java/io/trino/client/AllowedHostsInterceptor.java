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
package io.trino.client;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public final class AllowedHostsInterceptor
        implements Interceptor
{
    private final List<HostPattern> allowedHosts;
    private final boolean httpsRequired;

    public AllowedHostsInterceptor(Collection<String> allowedHosts, boolean httpsRequired)
    {
        this.allowedHosts = allowedHosts.stream()
                .map(HostPattern::parse)
                .collect(toImmutableList());
        this.httpsRequired = httpsRequired;
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        HttpUrl url = chain.request().url();
        if (httpsRequired && !url.isHttps()) {
            throw new HostNotAllowedException(format("Connecting to '%s' without TLS is not allowed when connecting to the server with TLS", url.host()));
        }
        if (!isAllowed(url.host())) {
            throw new HostNotAllowedException(format(
                    "Connecting to host '%s' is not allowed, add it to the allowedHosts connection property or the --allowed-hosts CLI option, " +
                            "or disable the restrictHosts connection property or the --restrict-hosts CLI option",
                    url.host()));
        }
        return chain.proceed(chain.request());
    }

    private boolean isAllowed(String host)
    {
        List<String> hostLabels = HostPattern.hostLabels(host);
        for (HostPattern pattern : allowedHosts) {
            if (pattern.matches(hostLabels)) {
                return true;
            }
        }
        return false;
    }
}
