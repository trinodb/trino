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

package io.prestosql.server.ui;

import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.http.client.HttpUriBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.HOST;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.X_FORWARDED_HOST;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PORT;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static io.airlift.http.client.HttpUriBuilder.uriBuilder;
import static javax.servlet.http.HttpServletResponse.SC_SEE_OTHER;

public class WebUiUtil
{
    public static final String UI_LOCATION = "/ui/";
    public static final String LOGIN_FORM = "/ui/login.html";

    private WebUiUtil()
    {
    }

    public static void sendRedirect(HttpServletResponse response, String location)
    {
        response.setHeader(LOCATION, location);
        response.setStatus(SC_SEE_OTHER);
    }

    public static String getLoginFormLocation(HttpServletRequest request)
    {
        return getRedirectLocation(request, LOGIN_FORM);
    }

    public static String getUiLocation(HttpServletRequest request)
    {
        return getRedirectLocation(request, UI_LOCATION);
    }

    public static String getRedirectLocation(HttpServletRequest request, String path)
    {
        return getRedirectLocation(request, path, null);
    }

    public static String getRedirectLocation(HttpServletRequest request, String path, String queryParameter)
    {
        HttpUriBuilder builder = toUriBuilderWithForwarding(request);
        builder.replacePath(path);
        if (queryParameter != null) {
            builder.addParameter(queryParameter);
        }
        return builder.toString();
    }

    public static HttpUriBuilder toUriBuilderWithForwarding(HttpServletRequest request)
    {
        HttpUriBuilder builder;
        if (isNullOrEmpty(request.getHeader(X_FORWARDED_PROTO)) && isNullOrEmpty(request.getHeader(X_FORWARDED_HOST))) {
            // not forwarded
            builder = uriBuilder()
                    .scheme(request.getScheme())
                    .hostAndPort(HostAndPort.fromString(request.getHeader(HOST)));
        }
        else {
            // forwarded
            builder = uriBuilder()
                    .scheme(firstNonNull(emptyToNull(request.getHeader(X_FORWARDED_PROTO)), request.getScheme()))
                    .hostAndPort(HostAndPort.fromString(firstNonNull(emptyToNull(request.getHeader(X_FORWARDED_HOST)), request.getHeader(HOST))));

            Optional.ofNullable(emptyToNull(request.getHeader(X_FORWARDED_PORT)))
                    .map(Ints::tryParse)
                    .ifPresent(builder::port);
        }
        return builder;
    }
}
