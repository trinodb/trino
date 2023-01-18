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
package io.trino.plugin.base.session;

import com.google.common.base.Functions;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Defaults.defaultValue;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class PropertyMetadataUtil
{
    private PropertyMetadataUtil() {}

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return dataSizeProperty(name, description, defaultValue, value -> {}, hidden);
    }

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, Consumer<DataSize> validation, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                DataSize.class,
                defaultValue,
                hidden,
                object -> {
                    DataSize value = DataSize.valueOf((String) object);
                    validation.accept(value);
                    return value;
                },
                DataSize::toString);
    }

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, boolean hidden)
    {
        return durationProperty(name, description, defaultValue, value -> {}, hidden);
    }

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, Consumer<Duration> validation, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                Duration.class,
                defaultValue,
                hidden,
                object -> {
                    Duration value = Duration.valueOf((String) object);
                    validation.accept(value);
                    return value;
                },
                Duration::toString);
    }

    public static PropertyBuilder property(String name)
    {
        return new PropertyBuilder(name);
    }

    public static class PropertyBuilder
    {
        private final String name;
        private String description;
        private Type sqlType;
        private Class<?> javaType;
        private @Nullable Object defaultValue;
        private boolean hidden;

        private PropertyBuilder(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @CanIgnoreReturnValue
        public <C, T> PropertyBuilder fromConfig(C config, Function<C, T> getterMethodReference)
        {
            Class<?> configClass = config.getClass();
            Method getter = findMethodByReference(configClass, configInstance -> {
                //noinspection unchecked
                T ignored = getterMethodReference.apply((C) configInstance);
            });
            description = findConfigDescriptionByGetter(configClass, getter)
                    .orElseThrow(() -> new IllegalArgumentException("Config associated with %s.%s has no description".formatted(configClass.getName(), getter.getName())));
            // Session properties are never primitives, as they are accessed with generic `ConnectorSession.getProperty(String, Class)`
            javaType = Primitives.wrap(getter.getReturnType());
            defaultValue = getterMethodReference.apply(config);

            if (javaType == Boolean.class) {
                sqlType = BOOLEAN;
            }
            if (javaType == Integer.class) {
                sqlType = INTEGER;
            }
            if (javaType == Long.class) {
                sqlType = BIGINT;
            }
            if (javaType == String.class) {
                sqlType = VARCHAR;
            }

            return this;
        }

        @CanIgnoreReturnValue
        public PropertyBuilder hidden()
        {
            hidden = true;
            return this;
        }

        public PropertyMetadata<?> build()
        {
            Function<Object, Object> decoder = Functions.identity();
            Function<Object, Object> encoder = Functions.identity();

            //noinspection unchecked,rawtypes
            return new PropertyMetadata(
                    name,
                    description,
                    sqlType,
                    javaType,
                    defaultValue,
                    hidden,
                    decoder,
                    encoder);
        }
    }

    private static <C> Method findMethodByReference(Class<C> clazz, Consumer<C> methodReference)
    {
        List<Method> calledMethods = new ArrayList<>(1);
        try (DynamicType.Unloaded<C> interceptingClass = new ByteBuddy()
                .subclass(clazz)
                .method(ElementMatchers.any())
                .intercept(MethodDelegation.to(new TrackCallsInterceptor(calledMethods::add)))
                .make()) {
            C interceptingConfigInstance = interceptingClass
                    .load(selectivelyChildFirstClassLoader(interceptingClass.getTypeDescription().getName(), clazz.getClassLoader()))
                    .getLoaded()
                    .getConstructor()
                    .newInstance();
            methodReference.accept(interceptingConfigInstance);
            return getOnlyElement(calledMethods);
        }
        catch (ReflectiveOperationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Child-first classloader for generated class, to avoid leak in ClassLoader.parallelLockMap within parent class loader.
     */
    private static ClassLoader selectivelyChildFirstClassLoader(String forClassName, ClassLoader parent)
    {
        requireNonNull(forClassName, "forClassName is null");
        return new URLClassLoader(new URL[0], parent)
        {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException
            {
                if (name.equals(forClassName)) {
                    return findClass(name);
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    public static class TrackCallsInterceptor
    {
        private final Consumer<Method> report;

        public TrackCallsInterceptor(Consumer<Method> report)
        {
            this.report = requireNonNull(report, "report is null");
        }

        @RuntimeType
        @SuppressWarnings("unused")
        public Object intercept(@Origin Method method, @AllArguments Object[] allArguments)
        {
            report.accept(method);
            return defaultValue(method.getReturnType());
        }
    }

    private static Optional<String> findConfigDescriptionByGetter(Class<?> clazz, Method getter)
    {
        String name = getter.getName();
        if (!name.startsWith("is") && !name.startsWith("get")) {
            throw new IllegalArgumentException("Not getter method: " + name);
        }
        String setterName = name.replaceFirst("^(is|get)", "set");

        Method setter = Stream.of(clazz.getMethods())
                .filter(method -> method.getName().equals(setterName))
                .filter(method -> method.isAnnotationPresent(Config.class))
                .collect(onlyElement());

        return Optional.ofNullable(setter.getAnnotation(ConfigDescription.class))
                .map(ConfigDescription::value);
    }
}
