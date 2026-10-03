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

import io.airlift.jaxrs.testing.MockUriInfo;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExternalUriInfo
{
    private static final URI REQUEST_URI = URI.create("http://localhost:8080/ui/login.html?redirectPath=%2Fui%2Fquery.html");

    @Test
    public void testAbsolutePath()
    {
        ExternalUriInfo uriInfo = externalUriInfo(null);

        assertThat(uriInfo.absolutePath("/ui/")).isEqualTo(URI.create("http://localhost:8080/ui/"));
    }

    @Test
    public void testAbsolutePathWithForwardedPrefix()
    {
        ExternalUriInfo uriInfo = externalUriInfo("/trino");

        assertThat(uriInfo.absolutePath("/ui/")).isEqualTo(URI.create("http://localhost:8080/trino/ui/"));
    }

    @Test
    public void testBaseUriBuilderWithRawQuery()
    {
        ExternalUriInfo uriInfo = externalUriInfo("/trino");

        // Form login passes the decoded path and query of the request it redirects from
        URI uri = uriInfo.baseUriBuilder()
                .path("/ui/login.html")
                .rawReplaceQuery("/ui/query.html?user=some user")
                .build();

        assertThat(uri).isEqualTo(URI.create("http://localhost:8080/trino/ui/login.html?/ui/query.html?user=some%20user"));
    }

    @Test
    public void testBaseUriBuilderWithFragment()
    {
        ExternalUriInfo uriInfo = externalUriInfo("/trino");

        URI uri = uriInfo.baseUriBuilder()
                .path("ui")
                .fragment("/queries/20260101_000000_00000_abcde")
                .build();

        assertThat(uri).isEqualTo(URI.create("http://localhost:8080/trino/ui#/queries/20260101_000000_00000_abcde"));
    }

    @Test
    public void testFullRequestUri()
    {
        ExternalUriInfo uriInfo = externalUriInfo(null);

        assertThat(uriInfo.fullRequestUri()).isEqualTo(REQUEST_URI);
    }

    @Test
    public void testFullRequestUriWithForwardedPrefix()
    {
        ExternalUriInfo uriInfo = externalUriInfo("/trino");

        assertThat(uriInfo.fullRequestUri())
                .isEqualTo(URI.create("http://localhost:8080/trino/ui/login.html?redirectPath=%2Fui%2Fquery.html"));
    }

    @Test
    public void testForBaseUri()
    {
        ExternalUriInfo uriInfo = ExternalUriInfo.forBaseUri(URI.create("https://example.net:8443"));

        assertThat(uriInfo.absolutePath("/v1/statement")).isEqualTo(URI.create("https://example.net:8443/v1/statement"));
        assertThat(uriInfo.fullRequestUri()).isEqualTo(URI.create("https://example.net:8443"));
    }

    /**
     * @param forwardedPrefix the {@code X-Forwarded-Prefix} header value, or null when the request has none
     */
    private static ExternalUriInfo externalUriInfo(String forwardedPrefix)
    {
        return new ExternalUriInfo(MockUriInfo.from(REQUEST_URI), forwardedPrefix);
    }
}
