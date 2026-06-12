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
package io.trino.plugin.doris;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDorisFeEndpoints
{
    @Test
    void testGetEndpointsSupportsExplicitScheme()
    {
        DorisConfig config = new DorisConfig().setFenodes("https://fe2.example.net:8443,http://fe1.example.net:8030");

        assertThat(DorisFeEndpoints.getEndpoints(config))
                .containsExactly(
                        new DorisFeEndpoints.FeEndpoint(Optional.of("http"), "fe1.example.net", 8030),
                        new DorisFeEndpoints.FeEndpoint(Optional.of("https"), "fe2.example.net", 8443));
    }

    @Test
    void testGetEndpointsSupportsHostPortEntries()
    {
        DorisConfig config = new DorisConfig().setFenodes("fe2.example.net:8030,fe1.example.net:8030");

        assertThat(DorisFeEndpoints.getEndpoints(config))
                .containsExactly(
                        new DorisFeEndpoints.FeEndpoint(Optional.empty(), "fe1.example.net", 8030),
                        new DorisFeEndpoints.FeEndpoint(Optional.empty(), "fe2.example.net", 8030));
    }

    @Test
    void testQueryPlanUrisFallbackToHttpAndHttpsForHostPortEntries()
    {
        DorisFeEndpoints.FeEndpoint endpoint = new DorisFeEndpoints.FeEndpoint(Optional.empty(), "fe.example.net", 8030);

        assertThat(endpoint.queryPlanUris("sales", "orders"))
                .containsExactly(
                        URI.create("http://fe.example.net:8030/api/sales/orders/_query_plan"),
                        URI.create("https://fe.example.net:8030/api/sales/orders/_query_plan"));
    }

    @Test
    void testQueryPlanUrisRespectExplicitScheme()
    {
        DorisFeEndpoints.FeEndpoint endpoint = new DorisFeEndpoints.FeEndpoint(Optional.of("https"), "fe.example.net", 8443);

        assertThat(endpoint.queryPlanUris("sales", "orders"))
                .containsExactly(URI.create("https://fe.example.net:8443/api/sales/orders/_query_plan"));
    }

    @Test
    void testQueryPlanUrisEncodeSchemaAndTableNamesAsPathSegments()
    {
        DorisFeEndpoints.FeEndpoint endpoint = new DorisFeEndpoints.FeEndpoint(Optional.of("https"), "fe.example.net", 8443);

        assertThat(endpoint.queryPlanUris("sales ops/2026", "orders?#daily"))
                .containsExactly(URI.create("https://fe.example.net:8443/api/sales%20ops%2F2026/orders%3F%23daily/_query_plan"));
    }

    @Test
    void testGetHostsIgnoresScheme()
    {
        DorisConfig config = new DorisConfig().setFenodes("https://fe1.example.net:8443,fe1.example.net:8030,fe2.example.net:8030");

        assertThat(DorisFeEndpoints.getHosts(config))
                .containsExactly("fe1.example.net", "fe2.example.net");
    }

    @Test
    void testInvalidSchemeFailsFast()
    {
        DorisConfig config = new DorisConfig().setFenodes("ftp://fe.example.net:21");

        assertThatThrownBy(() -> DorisFeEndpoints.getEndpoints(config))
                .isInstanceOf(TrinoException.class)
                .satisfies(throwable -> assertThat(((TrinoException) throwable).getErrorCode()).isEqualTo(CONFIGURATION_INVALID.toErrorCode()))
                .hasMessageContaining("scheme must be http or https");
    }

    @Test
    void testMissingFenodesFailsWithConfigurationInvalid()
    {
        DorisConfig config = new DorisConfig();

        assertThatThrownBy(() -> DorisFeEndpoints.getEndpoints(config))
                .isInstanceOf(TrinoException.class)
                .satisfies(throwable -> assertThat(((TrinoException) throwable).getErrorCode()).isEqualTo(CONFIGURATION_INVALID.toErrorCode()))
                .hasMessageContaining("doris.fenodes must be set");
    }
}
