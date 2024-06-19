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
package io.trino.server;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

public class RemoveForwardedPrefixFilter
        extends HttpFilter
{
    @Override
    protected void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException
    {
        if (request.getHeader("X-Forwarded-Prefix") != null) {
            request = new HttpServletRequestWrapper(request)
            {
                @Override
                public String getHeader(String name)
                {
                    if ("X-Forwarded-Prefix".equalsIgnoreCase(name)) {
                        return null; // Remove the header
                    }
                    return super.getHeader(name);
                }
            };
        }
        chain.doFilter(request, response);
    }
}
