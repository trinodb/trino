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
package org.apache.iceberg.rest;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.apache.hc.core5.http.HttpHeaders;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;

// TODO: remove once https://github.com/apache/iceberg/pull/17598 is included in the Iceberg dependency
public class QuotedETagRestCatalogServlet
        extends RESTCatalogServlet
{
    public QuotedETagRestCatalogServlet(RESTCatalogAdapter restCatalogAdapter)
    {
        super(restCatalogAdapter);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        super.doGet(withNormalizedIfNoneMatch(request), withQuotedETag(response));
    }

    @Override
    protected void doHead(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        super.doHead(withNormalizedIfNoneMatch(request), withQuotedETag(response));
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        super.doPost(withNormalizedIfNoneMatch(request), withQuotedETag(response));
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        super.doDelete(withNormalizedIfNoneMatch(request), withQuotedETag(response));
    }

    @Override
    protected void execute(ServletRequestContext context, HttpServletResponse response)
            throws IOException
    {
        super.execute(context, withQuotedETag(response));
    }

    private static HttpServletRequest withNormalizedIfNoneMatch(HttpServletRequest request)
    {
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public String getHeader(String name)
            {
                return normalizeIfNoneMatch(name, super.getHeader(name));
            }

            @Override
            public Enumeration<String> getHeaders(String name)
            {
                return Collections.enumeration(Collections.list(super.getHeaders(name)).stream()
                        .map(value -> normalizeIfNoneMatch(name, value))
                        .toList());
            }
        };
    }

    private static HttpServletResponse withQuotedETag(HttpServletResponse response)
    {
        return new HttpServletResponseWrapper(response)
        {
            @Override
            public void setHeader(String name, String value)
            {
                super.setHeader(name, quoteETag(name, value));
            }

            @Override
            public void addHeader(String name, String value)
            {
                super.addHeader(name, quoteETag(name, value));
            }
        };
    }

    private static String quoteETag(String name, String value)
    {
        if (!HttpHeaders.ETAG.equalsIgnoreCase(name) || value == null || isQuotedETag(value)) {
            return value;
        }
        return "\"" + value + "\"";
    }

    private static String normalizeIfNoneMatch(String name, String value)
    {
        if (!HttpHeaders.IF_NONE_MATCH.equalsIgnoreCase(name) || value == null || value.contains(",") || !isQuotedETag(value)) {
            return value;
        }
        if (value.startsWith("W/\"")) {
            return value.substring(3, value.length() - 1);
        }
        return value.substring(1, value.length() - 1);
    }

    private static boolean isQuotedETag(String value)
    {
        return (value.startsWith("\"") && value.endsWith("\"")) ||
                (value.startsWith("W/\"") && value.endsWith("\""));
    }
}
