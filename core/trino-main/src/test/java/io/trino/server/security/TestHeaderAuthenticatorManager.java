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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.HeaderAuthenticator;
import io.trino.spi.security.HeaderAuthenticatorFactory;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHeaderAuthenticatorManager
{
    @Test
    public void testMultipleConfigFiles()
            throws Exception
    {
        Path config1 = createTempFile("headerConfig", "1");
        Path config2 = createTempFile("headerConfig", "2");
        Files.write(config1, ImmutableList.of("header-authenticator.name=type1"));
        Files.write(config2, ImmutableList.of("header-authenticator.name=type2"));
        String trustedHeaderOne = "x-forwarded-client-cert";
        String trustedHeaderTwo = "forwarded-client-cert";
        ImmutableMap<String, List<String>> validRequestOne = ImmutableMap.of(trustedHeaderOne, ImmutableList.of("foo", "bar"));
        ImmutableMap<String, List<String>> validRequestTwo = ImmutableMap.of(trustedHeaderTwo, ImmutableList.of("cat", "dog"));
        ImmutableMap<String, List<String>> invalidRequestOne = ImmutableMap.of("try-hard-authn", ImmutableList.of("foo", "bar"));

        HeaderAuthenticatorManager manager = new HeaderAuthenticatorManager(new HeaderAuthenticatorConfig()
                .setHeaderAuthenticatorFiles(ImmutableList.of(config1.toAbsolutePath().toString(), config2.toAbsolutePath().toString())));
        manager.setRequired();

        manager.addHeaderAuthenticatorFactory(new TestingHeaderAuthenticatorFactory("type1", trustedHeaderOne));
        manager.addHeaderAuthenticatorFactory(new TestingHeaderAuthenticatorFactory("type2", trustedHeaderTwo));

        manager.loadHeaderAuthenticator();

        List<HeaderAuthenticator> authenticators = manager.getAuthenticators();
        assertThat(login(authenticators, validRequestOne::get)).isTrue();
        assertThat(login(authenticators, validRequestTwo::get)).isTrue();
        assertThat(login(authenticators, invalidRequestOne::get)).isFalse();
    }

    private boolean login(List<HeaderAuthenticator> authenticators, HeaderAuthenticator.Headers headers)
    {
        return authenticators.stream()
                .anyMatch(authenticator -> {
                    try {
                        authenticator.createAuthenticatedPrincipal(headers);
                        return true;
                    }
                    catch (AccessDeniedException e) {
                        return false;
                    }
                });
    }

    private static class TestingHeaderAuthenticatorFactory
            implements HeaderAuthenticatorFactory
    {
        private final String header;
        private final String name;

        TestingHeaderAuthenticatorFactory(String name, String header)
        {
            this.header = requireNonNull(header, "header is null");
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public HeaderAuthenticator create(Map<String, String> config)
        {
            return headers -> Optional.ofNullable(headers.getHeader(header))
                    .map(values -> new BasicPrincipal(values.get(0)))
                    .orElseThrow(() -> new AccessDeniedException("You shall not pass!"));
        }
    }
}
