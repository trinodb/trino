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
package io.trino.plugin.base.group;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.spi.security.GroupProvider;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * If added to the list of {@link com.google.inject.Module}s used in initialization of a Guice context in a
 * {@link io.trino.spi.security.GroupProviderFactory}, it will (almost) automatically add caching capability to the
 * group provider. Requirements:
 * <ul>
 *     <li>The {@link GroupProvider} available in the Guice context must be bound annotated with
 *     {@link ForCachingGroupProvider} binding annotation</li>
 * </ul>
 * The module will make the following configuration options available (to be set in {@code etc/group-provider.properties}:
 * <ul>
 *     <li>{@code cache.enabled} - the toggle to enable or disable caching</li>
 *     <li>{@code cache.ttl} - determines how long group information will be cached for each user</li>
 *     <li>{@code cache.maximum-size} - maximum number of users for which groups are stored in the cache</li>
 * </ul>
 * These properties can optionally have an arbitrary prefix ({@link Builder#withPrefix(String)})
 * and/or a binding annotation for the resulting binding of {@link GroupProvider} ({@link Builder#withBindingAnnotation(Class)}).
 * <p>
 * An additional object of type {@link GroupCacheInvalidationController} will also be bound, with which one can invalidate
 * all or part of the cache.
 */
public class CachingGroupProviderModule
        extends AbstractConfigurationAwareModule
{
    private final Optional<String> prefix;
    private final Optional<Class<? extends Annotation>> bindingAnnotation;

    private CachingGroupProviderModule(Optional<String> prefix, Optional<Class<? extends Annotation>> bindingAnnotation)
    {
        this.prefix = requireNonNull(prefix, "prefix is null");
        this.bindingAnnotation = requireNonNull(bindingAnnotation, "bindingAnnotation is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GroupProviderConfig.class, prefix.orElse(null));
        prefix.ifPresentOrElse(
                prefix -> install(conditionalModule(
                        GroupProviderConfig.class,
                        prefix,
                        GroupProviderConfig::isCachingEnabled,
                        new CacheModule(Optional.of(prefix), bindingAnnotation),
                        new NonCacheModule(bindingAnnotation))),
                () -> install(conditionalModule(
                        GroupProviderConfig.class,
                        GroupProviderConfig::isCachingEnabled,
                        new CacheModule(Optional.empty(), bindingAnnotation),
                        new NonCacheModule(bindingAnnotation))));
    }

    private static class CacheModule
            implements Module
    {
        private final Optional<String> prefix;
        private final Optional<Class<? extends Annotation>> bindingAnnotation;

        public CacheModule(Optional<String> prefix, Optional<Class<? extends Annotation>> bindingAnnotation)
        {
            this.prefix = requireNonNull(prefix, "prefix is null");
            this.bindingAnnotation = requireNonNull(bindingAnnotation, "bindingAnnotation is null");
        }

        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(CachingGroupProviderConfig.class, prefix.orElse(null));
            binder.bind(CachingGroupProvider.class).in(Scopes.SINGLETON);
            binder.bind(bindingAnnotation
                            .map(bindingAnnotation -> Key.get(GroupProvider.class, bindingAnnotation))
                            .orElseGet(() -> Key.get(GroupProvider.class)))
                    .to(CachingGroupProvider.class)
                    .in(Scopes.SINGLETON);
            binder.bind(GroupCacheInvalidationController.class)
                    .to(CachingGroupProvider.class)
                    .in(Scopes.SINGLETON);
        }
    }

    private static class NonCacheModule
            implements Module
    {
        private final Optional<Class<? extends Annotation>> bindingAnnotation;

        public NonCacheModule(Optional<Class<? extends Annotation>> bindingAnnotation)
        {
            this.bindingAnnotation = requireNonNull(bindingAnnotation, "bindingAnnotation is null");
        }

        @Override
        public void configure(Binder binder)
        {
            binder.bind(bindingAnnotation
                            .map(bindingAnnotation -> Key.get(GroupProvider.class, bindingAnnotation))
                            .orElseGet(() -> Key.get(GroupProvider.class)))
                    .to(Key.get(GroupProvider.class, ForCachingGroupProvider.class))
                    .in(Scopes.SINGLETON);
            binder.bind(GroupCacheInvalidationController.class)
                    .to(NoOpGroupCacheInvalidationController.class)
                    .in(Scopes.SINGLETON);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForCachingGroupProvider
    {
    }

    public static CachingGroupProviderModule create()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<String> prefix = Optional.empty();
        private Optional<Class<? extends Annotation>> bindingAnnotation = Optional.empty();

        private Builder() {}

        @CanIgnoreReturnValue
        public Builder withPrefix(String prefix)
        {
            this.prefix = Optional.of(prefix);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withBindingAnnotation(Class<? extends Annotation> bindingAnnotation)
        {
            this.bindingAnnotation = Optional.of(bindingAnnotation);
            return this;
        }

        public CachingGroupProviderModule build()
        {
            return new CachingGroupProviderModule(prefix, bindingAnnotation);
        }
    }
}
