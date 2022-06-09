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
package io.trino.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.lang.annotation.Annotation;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * This Guice {@link com.google.inject.Module} will bind an {@link AWSCredentialsProvider} in the
 * Guice created by {@link AwsCredentialsProviderFactory} based on the configuration available
 * in the context at a specified prefix. An optional annotation may be provided, which will
 * be used as a qualifier of the binding both of the {@link AWSCredentialsProvider} and the
 * {@link AwsCredentialsProviderConfig}. This way multiple {@link AWSCredentialsProvider}s can
 * be bound with different configurations.
 * <p>
 * This module requires {@link AwsCredentialsProviderFactoryModule}.
 */
public class AwsCredentialsProviderModule
        extends AbstractConfigurationAwareModule
{
    private final String configurationPrefix;
    private final Optional<Class<? extends Annotation>> qualifier;

    public AwsCredentialsProviderModule(String configurationPrefix)
    {
        this(configurationPrefix, Optional.empty());
    }

    public AwsCredentialsProviderModule(String configurationPrefix, Class<? extends Annotation> qualifier)
    {
        this(configurationPrefix, Optional.of(qualifier));
    }

    private AwsCredentialsProviderModule(String configurationPrefix, Optional<Class<? extends Annotation>> qualifier)
    {
        this.configurationPrefix = requireNonNull(configurationPrefix, "configurationPrefix is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        checkArgument(!configurationPrefix.isEmpty(), "prefix is empty");
    }

    @Override
    protected void setup(Binder binder)
    {
        qualifier.ifPresentOrElse(
                qualifier -> {
                    configBinder(binder).bindConfig(AwsCredentialsProviderConfig.class, qualifier, configurationPrefix);
                    binder.bind(Key.get(AWSCredentialsProvider.class, qualifier))
                            .toProvider(new AWSCredentialsProviderProvider(Key.get(AwsCredentialsProviderConfig.class, qualifier)))
                            .in(SINGLETON);
                },
                () -> {
                    configBinder(binder).bindConfig(AwsCredentialsProviderConfig.class, configurationPrefix);
                    binder.bind(Key.get(AWSCredentialsProvider.class))
                            .toProvider(new AWSCredentialsProviderProvider(Key.get(AwsCredentialsProviderConfig.class)))
                            .in(SINGLETON);
                });
    }

    private static class AWSCredentialsProviderProvider
            implements Provider<AWSCredentialsProvider>
    {
        private final Key<AwsCredentialsProviderConfig> configKey;
        private Injector injector;

        AWSCredentialsProviderProvider(Key<AwsCredentialsProviderConfig> configKey)
        {
            this.configKey = configKey;
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = requireNonNull(injector, "injector is null");
        }

        @Override
        public AWSCredentialsProvider get()
        {
            return injector.getInstance(AwsCredentialsProviderFactory.class)
                    .createAwsCredentialsProvider(injector.getInstance(configKey));
        }
    }
}
