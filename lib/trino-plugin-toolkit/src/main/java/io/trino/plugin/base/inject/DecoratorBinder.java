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
package io.trino.plugin.base.inject;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.airlift.log.Logger;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.util.Types.newParameterizedType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class DecoratorBinder<T>
{
    private final Multibinder<Decorator<T>> decorators;

    private DecoratorBinder(Binder binder, Class<T> clazz, Class<? extends Annotation> annotation)
    {
        //noinspection unchecked
        this.decorators = newSetBinder(binder, Key.get((TypeLiteral<Decorator<T>>) TypeLiteral.get(newParameterizedType(Decorator.class, clazz)), annotation));
        binder.install(new DecoratorModule<>(clazz, annotation));
    }

    public static <T> DecoratorBinder<T> newDecoratorBinder(Binder binder, Class<T> clazz, Class<? extends Annotation> annotation)
    {
        return new DecoratorBinder<>(binder, clazz, annotation);
    }

    public DecoratorBinder<T> addBinding(Class<? extends Decorator<T>> decoratorClass)
    {
        decorators.addBinding().to(decoratorClass).in(Scopes.SINGLETON);
        return this;
    }

    public DecoratorBinder<T> addInstanceBinding(Decorator<T> decoratorInstance)
    {
        decorators.addBinding().toInstance(decoratorInstance);

        return this;
    }

    public DecoratorBinder<T> addProviderBinding(Provider<Decorator<T>> decoratorProvider)
    {
        decorators.addBinding().toProvider(decoratorProvider);
        return this;
    }

    private static class DecoratingProvider<T>
            implements Provider<T>
    {
        private final Class<T> valueType;
        private final Class<? extends Annotation> valueAnnotation;
        private Injector injector;

        public DecoratingProvider(Class<T> valueType, Class<? extends Annotation> valueAnnotation)
        {
            this.valueType = requireNonNull(valueType, "valueType is null");
            this.valueAnnotation = requireNonNull(valueAnnotation, "valueAnnotation is null");
        }

        @Inject
        void setInjector(Injector injector)
        {
            this.injector = requireNonNull(injector, "injector is null");
        }

        @Override
        public T get()
        {
            checkState(injector != null, "injector wasn't injected");
            T instance = requireNonNull(injector.getInstance(Key.get(TypeLiteral.get(valueType), valueAnnotation)), "decorated instance is null");

            @SuppressWarnings("unchecked") List<Decorator<T>> decorators = sortedList((Set<Decorator<T>>)
                    injector.getInstance(Key.get(newParameterizedType(Set.class, newParameterizedType(Decorator.class, valueType)), valueAnnotation)));

            if (decorators.isEmpty()) {
                return instance;
            }

            Logger log = Logger.get(instance.getClass());

            if (log.isDebugEnabled()) {
                String separator = "\n\t-> ";
                String chainOfDecorators = decorators.stream()
                        .filter(decorator -> decorator.appliesTo(instance))
                        .map(decorator -> "%s [priority: %d]".formatted(decorator, decorator.priority()))
                        .collect(joining(separator));

                log.info("Decorating %s with decorators:%s%s", instance, separator, chainOfDecorators);
            }

            T currentValue = instance;
            for (Decorator<T> decorator : decorators) {
                if (decorator.appliesTo(instance)) {
                    currentValue = requireNonNull(decorator.apply(currentValue), () -> "decorator %s returned null value".formatted(decorator));
                }
            }
            return currentValue;
        }

        private static <T> List<Decorator<T>> sortedList(Set<Decorator<T>> decorators)
        {
            return decorators.stream()
                    .sorted()
                    .collect(toImmutableList());
        }
    }

    private static class DecoratorModule<T>
            implements Module
    {
        private final Class<T> valueType;
        private final Class<? extends Annotation> valueAnnotation;

        public DecoratorModule(Class<T> valueType, Class<? extends Annotation> valueAnnotation)
        {
            this.valueType = requireNonNull(valueType, "valueType is null");
            this.valueAnnotation = requireNonNull(valueAnnotation, "valueAnnotation is null");
        }

        @Override
        public void configure(Binder binder)
        {
            binder.bind(Key.get(valueType)).toProvider(new DecoratingProvider<>(valueType, valueAnnotation)).in(Scopes.SINGLETON);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DecoratorModule<?> that = (DecoratorModule<?>) o;

            if (!Objects.equals(valueType, that.valueType)) {
                return false;
            }
            return Objects.equals(valueAnnotation, that.valueAnnotation);
        }

        @Override
        public int hashCode()
        {
            int result = valueType != null ? valueType.hashCode() : 0;
            result = 31 * result + (valueAnnotation != null ? valueAnnotation.hashCode() : 0);
            return result;
        }
    }
}
