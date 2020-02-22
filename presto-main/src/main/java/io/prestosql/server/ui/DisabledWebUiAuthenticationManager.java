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

import com.google.common.io.ByteStreams;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.net.HttpHeaders.LOCATION;
import static javax.servlet.http.HttpServletResponse.SC_SEE_OTHER;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class DisabledWebUiAuthenticationManager
        implements WebUiAuthenticationManager
{
    private static final String DISABLED_LOCATION = "/ui/disabled.html";

    @Override
    public void handleUiRequest(HttpServletRequest request, HttpServletResponse response, FilterChain nextFilter)
            throws IOException, ServletException
    {
        if (request.getPathInfo() == null || request.getPathInfo().equals("/")) {
            response.setHeader(LOCATION, getRedirectLocation(request, DISABLED_LOCATION));
            response.setStatus(SC_SEE_OTHER);
            return;
        }

        if (isPublic(request)) {
            nextFilter.doFilter(request, response);
            return;
        }

        // drain the input
        try (InputStream inputStream = request.getInputStream()) {
            ByteStreams.exhaust(inputStream);
        }

        // send 401 to REST api calls and redirect to others
        if (request.getPathInfo().startsWith("/ui/api/")) {
            response.setStatus(SC_UNAUTHORIZED);
        }
        else {
            // redirect to login page
            response.setHeader(LOCATION, getRedirectLocation(request, DISABLED_LOCATION));
            response.setStatus(SC_SEE_OTHER);
        }
    }

    private static boolean isPublic(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo();
        return pathInfo.equals(DISABLED_LOCATION) ||
                pathInfo.startsWith("/ui/vendor") ||
                pathInfo.startsWith("/ui/assets");
    }

    private static String getRedirectLocation(HttpServletRequest request, String path)
    {
        return FormWebUiAuthenticationManager.getRedirectLocation(request, path, null);
    }
}
