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

import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriBuilderException;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.net.URISyntaxException;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * Provides external URI information for the current request. The external URI may have a path prefix when behind a reverse proxy.
 * The path prefix is extracted from the {@code X-Forwarded-Prefix} header.
 */
public class ExternalUriInfo
{
    private static final String X_FORWARDED_PREFIX = "X-Forwarded-Prefix";

    private final UriInfo uriInfo;
    private final String forwardedPrefix;

    public ExternalUriInfo(@Context UriInfo uriInfo, @HeaderParam(X_FORWARDED_PREFIX) String forwardedPrefix)
    {
        this.uriInfo = requireNonNull(uriInfo, "uriInfo is null");
        this.forwardedPrefix = requireNonNullElse(forwardedPrefix, "");
    }

    public static ExternalUriInfo from(ContainerRequestContext requestContext)
    {
        return new ExternalUriInfo(requestContext.getUriInfo(), requestContext.getHeaderString(X_FORWARDED_PREFIX));
    }

    /**
     * Returns an external URI builder with the forwarded path prefix and no query parameters.
     */
    public ExternalUriBuilder baseUriBuilder()
    {
        return new ExternalUriBuilder(uriInfo.getBaseUriBuilder()
                .replacePath(forwardedPrefix)
                .replaceQuery(""));
    }

    /**
     * Returns an external URI to the specified path prefixed with forwarded path prefix, and no query parameters.
     */
    public URI absolutePath(String path)
    {
        return baseUriBuilder().path(path).build();
    }

    /**
     * Returns the full request URI with the forwarded path prefix, and all query parameters.
     */
    public URI fullRequestUri()
    {
        return uriInfo.getRequestUriBuilder()
                .replacePath(forwardedPrefix)
                .path(uriInfo.getPath())
                .build();
    }

    public static class ExternalUriBuilder
    {
        private final UriBuilder uriBuilder;

        private ExternalUriBuilder(UriBuilder uriBuilder)
        {
            this.uriBuilder = requireNonNull(uriBuilder, "uriBuilder is null");
        }

        public ExternalUriBuilder path(String path)
        {
            uriBuilder.path(path);
            return this;
        }

        public ExternalUriBuilder replaceQuery(String query)
        {
            uriBuilder.replaceQuery(query);
            return this;
        }

        public ExternalUriBuilder rawReplaceQuery(String rawEncodedQuery)
        {
            // this is a hack - the replaceQuery method encodes the value where the uri method just copies the value
            try {
                uriBuilder.uri(new URI(null, null, null, rawEncodedQuery, null));
            }
            catch (URISyntaxException _) {
            }
            return this;
        }

        public URI build()
                throws IllegalArgumentException, UriBuilderException
        {
            return uriBuilder.build();
        }
    }
}
