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
package io.prestosql.client.auth.external;

import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAuthenticationAssembler
{
    private final AuthenticationAssembler assembler = new AuthenticationAssembler(uri("http://base.url"));

    @Test
    public void testAssemblyForEmptyHeader()
    {
        assertThatThrownBy(() -> assembler.toAuthentication(""))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("is null");
    }

    @Test
    public void testAssemblyWithAbsoluteUrls()
    {
        ExternalAuthentication authentication = assembler.toAuthentication("External-Bearer redirectUrl=\"http://redirect.url\", tokenUrl=\"http://token.url\"");

        assertThat(authentication.getRedirectUrl()).isEqualTo("http://redirect.url");
        assertThat(authentication.getTokenPath()).isEqualTo("http://token.url");
    }

    @Test
    public void testAssemblyWithRelativeUrls()
    {
        ExternalAuthentication authentication = new AuthenticationAssembler(uri("http://base.url"))
                .toAuthentication("External-Bearer redirectUrl=\"/redirect/url\", tokenUrl=\"/path/to/token\"");

        assertThat(authentication.getRedirectUrl()).isEqualTo("http://base.url/redirect/url");
        assertThat(authentication.getTokenPath()).isEqualTo("http://base.url/path/to/token");
    }

    private static URI uri(String uri)
    {
        try {
            return new URI(uri);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
