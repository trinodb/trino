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
package io.trino.client.auth.external;

import io.trino.client.ClientException;
import org.testng.annotations.Test;

import java.net.URISyntaxException;

import static io.trino.client.auth.external.AuthenticationAssembler.REDIRECT_URI_FIELD;
import static io.trino.client.auth.external.AuthenticationAssembler.TOKEN_URI_FIELD;
import static java.lang.String.format;
import static java.net.URI.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAuthenticationAssembler
{
    @Test
    public void testAssemblyForHeaderWithBearerOnlyPrefix()
    {
        assertThatThrownBy(() -> AuthenticationAssembler.toAuthentication("Bearer"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Header \"Bearer\" does not contain %s or %s fields", REDIRECT_URI_FIELD, TOKEN_URI_FIELD);
    }

    @Test
    public void testAssemblyForHeaderWithoutBearerPrefix()
    {
        assertThatThrownBy(() -> AuthenticationAssembler.toAuthentication("x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bearer header required, \"x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\"\" does not start with bearer prefix");
    }

    @Test
    public void testAssemblyForHeaderWithValuesWithoutQuotationMarks()
    {
        assertThatThrownBy(() -> AuthenticationAssembler.toAuthentication("Bearer x_redirect_server=http://redirect.uri, x_token_server=http://token.uri"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Fields are required to be in quotation marks");
    }

    @Test
    public void testAssemblyWithBothUri()
    {
        ExternalAuthentication authentication = AuthenticationAssembler.toAuthentication("Bearer x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\"");

        assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
        assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
    }

    @Test
    public void testAssemblyWithAdditionalFields()
    {
        ExternalAuthentication authentication = AuthenticationAssembler.toAuthentication(
                "Bearer type=\"token\", x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\", description=\"oauth challenge\"");

        assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
        assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
    }

    @Test
    public void testAssemblyWithFieldsWithoutCommaSeparator()
    {
        assertThatThrownBy(() -> AuthenticationAssembler.toAuthentication("Bearer x_redirect_server=\"http://redirect.uri\"=x_token_server=\"http://token.uri\""))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining("Parsing field \"x_redirect_server\" to URI has failed");
    }

    @Test
    public void testAssemblyWithOnlyTokenServerUri()
    {
        ExternalAuthentication authentication = AuthenticationAssembler.toAuthentication("Bearer x_token_server=\"http://token.uri\"");

        assertThat(authentication.getRedirectUri()).isEmpty();
        assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
    }

    @Test
    public void testAssemblyWithMalformedUri()
    {
        assertThatThrownBy(() -> AuthenticationAssembler.toAuthentication("Bearer x_token_server=\"::::\""))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining(format("Parsing field \"%s\" to URI has failed", TOKEN_URI_FIELD))
                .hasCauseInstanceOf(URISyntaxException.class);
    }
}
