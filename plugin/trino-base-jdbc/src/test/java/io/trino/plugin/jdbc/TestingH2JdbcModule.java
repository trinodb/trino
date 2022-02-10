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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import org.h2.Driver;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingH2JdbcModule
        implements Module
{
    private final TestingH2JdbcClientFactory testingH2JdbcClientFactory;

    public TestingH2JdbcModule()
    {
        this((config, connectionFactory, identifierMapping) -> new TestingH2JdbcClient(config, connectionFactory, identifierMapping));
    }

    public TestingH2JdbcModule(TestingH2JdbcClientFactory testingH2JdbcClientFactory)
    {
        this.testingH2JdbcClientFactory = requireNonNull(testingH2JdbcClientFactory, "testingH2JdbcClientFactory is null");
    }

    @Override
    public void configure(Binder binder) {}

    @Provides
    @Singleton
    @ForBaseJdbc
    public JdbcClient provideJdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, IdentifierMapping identifierMapping)
    {
        return testingH2JdbcClientFactory.create(config, connectionFactory, identifierMapping);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new Driver(), config, credentialProvider);
    }

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime() + ThreadLocalRandom.current().nextLong()))
                .buildOrThrow();
    }

    public interface TestingH2JdbcClientFactory
    {
        TestingH2JdbcClient create(BaseJdbcConfig config, ConnectionFactory connectionFactory, IdentifierMapping identifierMapping);
    }
}
