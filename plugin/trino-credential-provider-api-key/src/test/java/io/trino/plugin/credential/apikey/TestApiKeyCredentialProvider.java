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
package io.trino.plugin.credential.apikey;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.credential.BearerTokenCredential;
import io.trino.spi.security.credential.CredentialProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestApiKeyCredentialProvider
{
    @Test
    public void test()
    {
        ApiKeyCredentialProviderFactory factory = new ApiKeyCredentialProviderFactory();
        assertThat(factory.getFactoryName()).isEqualTo("api_key");

        CredentialProvider provider = factory.create("my-name", ImmutableMap.of("api-key", "the-key-value"));
        assertThat(provider).isInstanceOf(ApiKeyCredentialProvider.class);

        ConnectorIdentity identity = ConnectorIdentity.ofUser("alice");
        BearerTokenCredential credential = provider.getCredential(identity, BearerTokenCredential.class);
        assertThat(credential.bearerToken()).isEqualTo("the-key-value");

        assertThat(credential.getHeaders()).isEqualTo(ImmutableMap.of("Authorization", "Bearer the-key-value"));
    }
}
