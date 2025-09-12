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
import java.util.logging.Level;
import java.util.logging.Logger;

public class SafeRedirector
        implements Interceptor
{
    private static final Logger log = Logger.getLogger(SafeRedirector.class.getName());

    public SafeRedirector() {}

    static boolean deny(InetAddress addr)
    {
        return addr.isAnyLocalAddress() ||
               addr.isLoopbackAddress() ||
               addr.isLinkLocalAddress() ||
               addr.isSiteLocalAddress();
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        Response response = chain.proceed(chain.request());

        if (response.isRedirect()) {
            String location = response.header("Location");
            if (location != null) {
                validateRedirect(location);
            }
        }
        return response;
    }

    void validateRedirect(String location)
    {
        URI redirectUri;
        try {
            redirectUri = new URI(location);
        }
        catch (URISyntaxException e) {
            log.log(Level.SEVERE, "Failed to parse redirect location " + location, e);
            // This should fail later, let it fail in the normal location.
            return;
        }

        String host = redirectUri.getHost();
        if (host == null) {
            log.warning("Redirect location " + location + " has no host");
            return;
        }

        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress address : addresses) {
                if (deny(address)) {
                    log.warning("Blocked redirect to " + location + " (resolved to " + address + ")");
                    throw new ClientException("Redirect to location is not allowed: " + location);
                }
            }
        }
        catch (UnknownHostException e) {
            log.log(Level.SEVERE, "Failed to resolve redirect host " + host, e);
            // This should fail later, let it fail in the normal location.
        }
    }
}
