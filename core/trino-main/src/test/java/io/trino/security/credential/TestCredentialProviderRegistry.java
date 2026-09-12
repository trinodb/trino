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
package io.trino.security.credential;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.slice.Slice;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.spi.security.credential.Credential;
import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.CredentialProviderFactory;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.SqlPath;
import io.trino.sql.query.QueryAssertions;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static io.trino.plugin.base.security.credential.CredentialProviderModule.credentialProvider;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.security.credential.CredentialProvider.assertSupportedTypes;
import static io.trino.spi.type.TypeDescriptor.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class TestCredentialProviderRegistry
{
    @Test
    public void test()
    {
        try (QueryAssertions assertions = new QueryAssertions(testSessionBuilder()
                .setPath(SqlPath.buildPath("test_catalog_name.test_schema_name", Optional.empty()))
                .setIdentity(Identity.ofUser("test_user"))
                .build())) {
            assertions.addPlugin(new TestPlugin());
            assertions.addCredentialProvider("my-configured-provider", "test_provider_factory_name", Map.of("username", "admin", "password", "welcome123"));
            assertions.getQueryRunner().createCatalog("test_catalog_name", "test_connector_name", Map.of("credential-provider.my-code-named-provider.name", "my-configured-provider"));
            assertions.function("greeting", "'hello'")
                    .assertThat()
                    .isEqualTo("hello world from test_user as admin: welcome123");
        }
    }

    public static class TestPlugin
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableSet.of(new TestConnectorFactory());
        }

        @Override
        public Iterable<CredentialProviderFactory> getCredentialProviderFactories()
        {
            return ImmutableList.of(new TestCredentialProviderFactory());
        }
    }

    public static class TestConnectorFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return "test_connector_name";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            checkStrictSpiVersionMatch(context, this);

            Bootstrap app = new Bootstrap(
                    "io.trino.bootstrap.catalog." + catalogName,
                    new TestModule(),
                    new ConnectorContextModule(catalogName, context));

            Injector injector = app
                    .doNotInitializeLogging()
                    .disableSystemProperties()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(Connector.class);
        }
    }

    public enum TestTransactionHandle
            implements ConnectorTransactionHandle
    {
        INSTANCE
    }

    public static class TestMetadata
            implements ConnectorMetadata
    {
        private static final String SCHEMA_NAME = "test_schema_name";

        private final List<FunctionMetadata> functions;

        @Inject
        public TestMetadata(List<FunctionMetadata> functions)
        {
            this.functions = ImmutableList.copyOf(requireNonNull(functions, "functions is null"));
        }

        @Override
        public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
        {
            return schemaName.equals(SCHEMA_NAME) ? functions : List.of();
        }

        @Override
        public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
        {
            if (!name.schemaName().equals(SCHEMA_NAME)) {
                return List.of();
            }
            return functions.stream()
                    .filter(function -> function.getCanonicalName().equals(name.functionName()))
                    .toList();
        }

        @Override
        public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
        {
            return functions.stream()
                    .filter(function -> function.getFunctionId().equals(functionId))
                    .findFirst()
                    .orElseThrow();
        }

        @Override
        public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
        {
            return FunctionDependencyDeclaration.builder()
                    .addType(mapType(VARCHAR.getTypeDescriptor(), VARCHAR.getTypeDescriptor()))
                    .build();
        }
    }

    public static class TestConnector
            implements Connector
    {
        private final FunctionProvider functionProvider;
        private final ConnectorMetadata metadata;

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return TestTransactionHandle.INSTANCE;
        }

        @Inject
        public TestConnector(FunctionProvider functionProvider, ConnectorMetadata metadata)
        {
            this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Optional<FunctionProvider> getFunctionProvider()
        {
            return Optional.of(functionProvider);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public void shutdown() {}
    }

    public static class TestModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            credentialProvider(binder, "my-code-named-provider");
            binder.bind(TestConnector.class).in(SINGLETON);
            binder.bind(Connector.class).to(TestConnector.class).in(SINGLETON);

            binder.bind(TestMetadata.class).in(SINGLETON);
            binder.bind(ConnectorMetadata.class).to(TestMetadata.class).in(SINGLETON);

            binder.bind(TestFunctionProvider.class).in(SINGLETON);
            binder.bind(FunctionProvider.class).to(TestFunctionProvider.class).in(SINGLETON);
        }

        @Provides
        public static List<FunctionMetadata> getFunctionMetadata(TestFunctionProvider functions)
        {
            return functions.getFunctions();
        }
    }

    public static class TestFunctionProvider
            implements FunctionProvider
    {
        private final MethodHandle methodHandle;
        private static final List<FunctionMetadata> FUNCTIONS = ImmutableList.of(FunctionMetadata.scalarBuilder("greeting")
                .description("Greet the planet")
                .signature(Signature.builder()
                        .returnType(VARCHAR.getTypeDescriptor())
                        .argumentTypes(List.of(VARCHAR.getTypeDescriptor()))
                        .build())
                .build());

        private final CredentialProvider credentialProvider;

        @Inject
        public TestFunctionProvider(@Named("my-code-named-provider") CredentialProvider credentialProvider)
                throws Exception
        {
            this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
            methodHandle = lookup().findVirtual(TestFunctionProvider.class, "greeting", methodType(Slice.class, ConnectorSession.class, Slice.class));
        }

        public List<FunctionMetadata> getFunctions()
        {
            return FUNCTIONS;
        }

        @Override
        public ScalarFunctionImplementation getScalarFunctionImplementation(
                FunctionId functionId,
                BoundSignature boundSignature,
                FunctionDependencies functionDependencies,
                InvocationConvention invocationConvention)
        {
            return ScalarFunctionImplementation.builder()
                    .methodHandle(ScalarFunctionAdapter.adapt(
                            this.methodHandle.bindTo(this),
                            boundSignature.getReturnType(),
                            boundSignature.getArgumentTypes(),
                            new InvocationConvention(
                                    nCopies(boundSignature.getArity(), NEVER_NULL),
                                    FAIL_ON_NULL,
                                    true,
                                    false),
                            invocationConvention))
                    .build();
        }

        public Slice greeting(ConnectorSession session, Slice greeting)
        {
            TestCredential credential = credentialProvider.getCredential(session.getIdentity(), TestCredential.class).orElseThrow();
            return utf8Slice("%s world from %s: %s".formatted(greeting.toStringUtf8(), credential.username(), credential.password()));
        }
    }

    public record TestCredential(String username, String password)
            implements Credential {}

    public static class TestCredentialProvider
            implements CredentialProvider
    {
        private final TestCredentialProviderConfig config;

        @Inject
        public TestCredentialProvider(TestCredentialProviderConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public <T extends Credential> Optional<T> getCredential(ConnectorIdentity identity, Class<T> type)
        {
            assertSupportedTypes(this, type, TestCredential.class);
            return Optional.of((T) new TestCredential("%s as %s".formatted(identity.getUser(), config.getUsername()), config.getPassword()));
        }
    }

    public static class TestCredentialProviderConfig
    {
        private String username;
        private String password;

        @NotNull
        public String getUsername()
        {
            return username;
        }

        @Config("username")
        public TestCredentialProviderConfig setUsername(String username)
        {
            this.username = username;
            return this;
        }

        @NotNull
        public String getPassword()
        {
            return password;
        }

        @Config("password")
        @ConfigSecuritySensitive
        public TestCredentialProviderConfig setPassword(String password)
        {
            this.password = password;
            return this;
        }
    }

    public static class TestCredentialProviderFactory
            implements CredentialProviderFactory
    {
        @Override
        public String getFactoryName()
        {
            return "test_provider_factory_name";
        }

        @Override
        public CredentialProvider create(String providerName, Map<String, String> config)
        {
            Bootstrap app = new Bootstrap(
                    "io.trino.bootstrap.credential-provider.%s".formatted(providerName),
                    new TestCredentialProviderModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .disableSystemProperties()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(TestCredentialProvider.class);
        }
    }

    public static class TestCredentialProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(TestCredentialProviderConfig.class);
            binder.bind(TestCredentialProvider.class).in(SINGLETON);
        }
    }
}
