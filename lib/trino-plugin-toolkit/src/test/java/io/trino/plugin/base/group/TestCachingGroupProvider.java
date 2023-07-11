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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.group.CachingGroupProviderModule.ForCachingGroupProvider;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.GroupProviderFactory;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCachingGroupProvider
{
    @Test
    public void testWithOutCaching()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "cache.enabled", "false");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.empty(), Optional.empty()).create(properties);

        innerTestWithOutCaching(countingGroupProvider, groupProvider);
    }

    @Test
    public void testWithOutCachingWithBindingAnnotation()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "cache.enabled", "false");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.empty(), Optional.of(ForTesting.class)).create(properties);

        innerTestWithOutCaching(countingGroupProvider, groupProvider);
    }

    @Test
    public void testWithOutCachingWithPrefix()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "group-provider.cache.enabled", "false");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.of("group-provider"), Optional.empty()).create(properties);

        innerTestWithOutCaching(countingGroupProvider, groupProvider);
    }

    @Test
    public void testWithOutCachingWithPrefixWithBindingAnnotation()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "group-provider.cache.enabled", "false");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.of("group-provider"), Optional.of(ForTesting.class)).create(properties);

        innerTestWithOutCaching(countingGroupProvider, groupProvider);
    }

    private static void innerTestWithOutCaching(CountingGroupProvider countingGroupProvider, TestingGroupProvider groupProvider)
    {
        assertThat(countingGroupProvider.getCount()).isEqualTo(0);

        // first batch
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(1);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(2);

        // second batch
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(3);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(4);

        // invalidate user
        groupProvider.invalidate("testUser1");
        // no effect:
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(5);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(6);

        // invalidate all
        groupProvider.invalidateAll();
        // no effect:
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(7);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(8);
    }

    @Test
    public void testWithCaching()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "cache.enabled", "true",
                "cache.ttl", "1 h");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.empty(), Optional.empty()).create(properties);

        innerTestWithCaching(countingGroupProvider, groupProvider);
    }

    @Test
    public void testWithCachingWithBindingAnnotation()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "cache.enabled", "true",
                "cache.ttl", "1 h");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.empty(), Optional.of(ForTesting.class)).create(properties);

        innerTestWithCaching(countingGroupProvider, groupProvider);
    }

    @Test
    public void testWithCachingWithPrefix()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "group-provider.cache.enabled", "true",
                "group-provider.cache.ttl", "1 h");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.of("group-provider"), Optional.empty()).create(properties);

        innerTestWithCaching(countingGroupProvider, groupProvider);
    }

    @Test
    public void testWithCachingWithPrefixWithBindingAnnotation()
    {
        CountingGroupProvider countingGroupProvider = new CountingGroupProvider();
        Map<String, String> properties = ImmutableMap.of(
                "group-provider.cache.enabled", "true",
                "group-provider.cache.ttl", "1 h");
        TestingGroupProvider groupProvider = new TestingGroupProviderFactory(countingGroupProvider, Optional.of("group-provider"), Optional.of(ForTesting.class)).create(properties);

        innerTestWithCaching(countingGroupProvider, groupProvider);
    }

    private static void innerTestWithCaching(CountingGroupProvider countingGroupProvider, TestingGroupProvider groupProvider)
    {
        assertThat(countingGroupProvider.getCount()).isEqualTo(0);

        // first batch
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(1);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(2);

        // second batch is handled by the cache so delegate not invoked
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(2);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(2);

        // invalidate user
        groupProvider.invalidate("testUser1");
        // effect on testUser1 only:
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(3);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(3);

        // invalidate all
        groupProvider.invalidateAll();
        // effect on both:
        assertThat(groupProvider.getGroups("testUser1")).containsOnly("test", "testUser1");
        assertThat(countingGroupProvider.getCount()).isEqualTo(4);
        assertThat(groupProvider.getGroups("testUser2")).containsOnly("test", "testUser2");
        assertThat(countingGroupProvider.getCount()).isEqualTo(5);
    }

    private static class CountingGroupProvider
            implements GroupProvider
    {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Set<String> getGroups(String user)
        {
            counter.incrementAndGet();
            return ImmutableSet.of("test", user);
        }

        public int getCount()
        {
            return counter.get();
        }
    }

    private static class TestingGroupProviderFactory
            implements GroupProviderFactory
    {
        private final CountingGroupProvider groupProvider;
        private final Optional<String> prefix;
        private final Optional<Class<? extends Annotation>> bindingAnnotation;

        private TestingGroupProviderFactory(CountingGroupProvider groupProvider, Optional<String> prefix, Optional<Class<? extends Annotation>> bindingAnnotation)
        {
            this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
            this.prefix = requireNonNull(prefix, "prefix is null");
            this.bindingAnnotation = requireNonNull(bindingAnnotation, "bindingAnnotation is null");
        }

        @Override
        public String getName()
        {
            return "counting";
        }

        @Override
        public TestingGroupProvider create(Map<String, String> config)
        {
            CachingGroupProviderModule.Builder moduleBuilder = CachingGroupProviderModule.builder();
            prefix.ifPresent(moduleBuilder::withPrefix);
            bindingAnnotation.ifPresent(moduleBuilder::withBindingAnnotation);

            Bootstrap app = new Bootstrap(
                    moduleBuilder.build(),
                    binder -> {
                        binder.bind(Key.get(GroupProvider.class, ForCachingGroupProvider.class))
                                .toInstance(groupProvider);
                        bindingAnnotation.ifPresent(bindingAnnotation ->
                                binder.bind(GroupProvider.class).to(Key.get(GroupProvider.class, bindingAnnotation)));
                        binder.bind(TestingGroupProvider.class);
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(TestingGroupProvider.class);
        }
    }

    private static class TestingGroupProvider
            implements GroupProvider, GroupCacheInvalidationController
    {
        private final GroupProvider delegate;
        private final GroupCacheInvalidationController invalidationController;

        @Inject
        public TestingGroupProvider(GroupProvider delegate, GroupCacheInvalidationController invalidationController)
        {
            this.delegate = requireNonNull(delegate, "delegate");
            this.invalidationController = requireNonNull(invalidationController, "invalidationController is null");
        }

        @Override
        public Set<String> getGroups(String user)
        {
            return delegate.getGroups(user);
        }

        @Override
        public void invalidate(String user)
        {
            invalidationController.invalidate(user);
        }

        @Override
        public void invalidateAll()
        {
            invalidationController.invalidateAll();
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    private @interface ForTesting
    {
    }
}
