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

import okhttp3.Interceptor;
import okhttp3.Response;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static java.lang.String.format;

public class DisallowLocalRedirectInterceptor
        implements Interceptor
{
    public DisallowLocalRedirectInterceptor() {}

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        Response response = chain.proceed(chain.request());
        if (response.isRedirect()) {
            String location = response.header("Location");
            if (!redirectAllowed(location)) {
                throw new ClientException(format("Following redirect to '%s' is disallowed", location));
            }
        }
        return response;
    }

    boolean redirectAllowed(String location)
    {
        if (location == null) {
            return true;
        }

        try {
            String host = new URI(location).getHost();
            if (host == null) {
                return true;
            }
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress address : addresses) {
                if (isLocalAddress(address)) {
                    return false;
                }
            }
        }
        catch (URISyntaxException | UnknownHostException ignored) {
            // This will fail later anyway
        }
        return true;
    }

    static boolean isLocalAddress(InetAddress addr)
    {
        return addr.isAnyLocalAddress() ||
                addr.isLoopbackAddress() ||
                addr.isLinkLocalAddress() ||
                addr.isSiteLocalAddress();
    }
}
