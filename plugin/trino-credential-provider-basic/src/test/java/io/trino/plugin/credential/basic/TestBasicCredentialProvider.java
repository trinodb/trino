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
package io.trino.plugin.credential.basic;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.credential.BasicCredential;
import io.trino.spi.security.credential.CredentialProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBasicCredentialProvider
{
    @Test
    public void test()
    {
        BasicCredentialProviderFactory factory = new BasicCredentialProviderFactory();
        assertThat(factory.getFactoryName()).isEqualTo("basic");

        CredentialProvider provider = factory.create("my-name", ImmutableMap.of("username", "admin", "password", "hunter2"));
        assertThat(provider).isInstanceOf(BasicCredentialProvider.class);

        ConnectorIdentity identity = ConnectorIdentity.ofUser("alice");
        BasicCredential credential = provider.getCredential(identity, BasicCredential.class);
        assertThat(credential.username()).isEqualTo("admin");
        assertThat(credential.password()).isEqualTo("hunter2");

        assertThat(credential.getHeaders()).isEqualTo(ImmutableMap.of("Authorization", "Basic YWRtaW46aHVudGVyMg=="));
    }
}
