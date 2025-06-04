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
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.testing.TempFile;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.GroupProviderFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGroupProviderManager
{
    private static final GroupProvider TEST_GROUP_PROVIDER = user -> ImmutableSet.of("test", user);
    private static final GroupProviderFactory TEST_GROUP_PROVIDER_FACTORY = new GroupProviderFactory()
    {
        @Override
        public String getName()
        {
            return "testGroupProvider";
        }

        @Override
        public GroupProvider create(Map<String, String> config)
        {
            return TEST_GROUP_PROVIDER;
        }
    };

    @Test
    public void testGroupProviderIsLoaded()
            throws IOException
    {
        try (TempFile tempFile = new TempFile()) {
            Files.write(tempFile.path(), "group-provider.name=testGroupProvider".getBytes(UTF_8));
            GroupProviderManager groupProviderManager = new GroupProviderManager(new SecretsResolver(ImmutableMap.of()));
            groupProviderManager.addGroupProviderFactory(TEST_GROUP_PROVIDER_FACTORY);
            groupProviderManager.loadConfiguredGroupProvider(tempFile.file());
            assertThat(groupProviderManager.getGroups("alice")).isEqualTo(ImmutableSet.of("test", "alice"));
            assertThat(groupProviderManager.getGroups("bob")).isEqualTo(ImmutableSet.of("test", "bob"));
        }
    }
}
