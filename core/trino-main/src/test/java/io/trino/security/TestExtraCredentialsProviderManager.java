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
package io.trino.security;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.testing.TempFile;
import io.trino.spi.security.ExtraCredentialsProvider;
import io.trino.spi.security.ExtraCredentialsProviderFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExtraCredentialsProviderManager
{
    private static final ExtraCredentialsProvider TEST_EXTRA_CREDENTIALS_PROVIDER = user -> ImmutableMap.of("db.user", user, "db.password", user + "-secret");
    private static final ExtraCredentialsProviderFactory TEST_EXTRA_CREDENTIALS_PROVIDER_FACTORY = new ExtraCredentialsProviderFactory()
    {
        @Override
        public String getName()
        {
            return "testExtraCredentialsProvider";
        }

        @Override
        public ExtraCredentialsProvider create(Map<String, String> config)
        {
            return TEST_EXTRA_CREDENTIALS_PROVIDER;
        }
    };

    @Test
    void testExtraCredentialsProviderIsLoaded()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Files.writeString(tempFile.path(),
                    """
                    extra-credentials-provider.name=testExtraCredentialsProvider
                    """);

            ExtraCredentialsProviderManager manager = createManager();
            manager.loadConfiguredExtraCredentialsProvider(tempFile.file());

            assertThat(manager.getExtraCredentials("Alice"))
                    .isEqualTo(ImmutableMap.of("db.user", "Alice", "db.password", "Alice-secret"));
            assertThat(manager.getExtraCredentials("Bob"))
                    .isEqualTo(ImmutableMap.of("db.user", "Bob", "db.password", "Bob-secret"));
        }
    }

    @Test
    void testNoProviderConfiguredReturnsNoCredentials()
    {
        assertThat(createManager().getExtraCredentials("Alice")).isEmpty();
    }

    @Test
    void testMissingConfigurationFileIsIgnored()
            throws Exception
    {
        ExtraCredentialsProviderManager manager = createManager();
        manager.loadConfiguredExtraCredentialsProvider(new File("/does/not/exist/extra-credentials-provider.properties"));

        assertThat(manager.getExtraCredentials("Alice")).isEmpty();
    }

    @Test
    void testMissingNameProperty()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Files.writeString(tempFile.path(),
                    """
                    some-property=some-value
                    """);

            ExtraCredentialsProviderManager manager = createManager();

            assertThatThrownBy(() -> manager.loadConfiguredExtraCredentialsProvider(tempFile.file()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(format(
                            "Extra credentials provider configuration %s does not contain extra-credentials-provider.name",
                            tempFile.path().toAbsolutePath()));
        }
    }

    @Test
    void testUnregisteredProvider()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Files.writeString(tempFile.path(),
                    """
                    extra-credentials-provider.name=unknown
                    """);

            ExtraCredentialsProviderManager manager = createManager();

            assertThatThrownBy(() -> manager.loadConfiguredExtraCredentialsProvider(tempFile.file()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Extra credentials provider unknown is not registered");
        }
    }

    @Test
    void testDuplicateFactoryRegistration()
    {
        ExtraCredentialsProviderManager manager = createManager();

        assertThatThrownBy(() -> manager.addExtraCredentialsProviderFactory(TEST_EXTRA_CREDENTIALS_PROVIDER_FACTORY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Extra credentials provider 'testExtraCredentialsProvider' is already registered");
    }

    private static ExtraCredentialsProviderManager createManager()
    {
        ExtraCredentialsProviderManager manager = new ExtraCredentialsProviderManager(new SecretsResolver(ImmutableMap.of()));
        manager.addExtraCredentialsProviderFactory(TEST_EXTRA_CREDENTIALS_PROVIDER_FACTORY);
        return manager;
    }
}
