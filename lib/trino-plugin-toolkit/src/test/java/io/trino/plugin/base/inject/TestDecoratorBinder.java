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
import com.google.inject.BindingAnnotation;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Retention;
import java.util.List;
import java.util.function.Function;

import static io.trino.plugin.base.inject.DecoratorBinder.newDecoratorBinder;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDecoratorBinder
{
    @BeforeAll
    public static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel(String.class.getName(), Level.DEBUG);
    }

    @Test
    public void testNoDecorators()
    {
        assertFinalValue("test", "test", List.of());
    }

    @Test
    public void testSingleDecorator()
    {
        assertFinalValue("test", "TEST", List.of(new Transformation(1, value -> value.toUpperCase(ENGLISH))));
    }

    @Test
    public void testMultipleDecorator()
    {
        assertFinalValue("test", "second(first(test))", List.of(
                new Transformation(1, value -> "first(" + value + ")"),
                new Transformation(2, value -> "second(" + value + ")")));
    }

    @Test
    public void testMultipleDecoratorSortedByPriority()
    {
        assertFinalValue("test", "SECond(firST(test))", List.of(
                new Transformation(2, value -> "SECond(" + value + ")"),
                new Transformation(1, value -> "firST(" + value + ")")));

        assertFinalValue("test2", "3rd(second(FIR_ST(test2)))", List.of(
                new Transformation(2, value -> "second(" + value + ")"),
                new Transformation(1, value -> "FIR_ST(" + value + ")"),
                new Transformation(3, value -> "3rd(" + value + ")")));

        assertFinalValue("test3", "third(second(second(first(test3))))", List.of(
                new Transformation(2, value -> "second(" + value + ")"),
                new Transformation(2, value -> "second(" + value + ")"),
                new Transformation(1, value -> "first(" + value + ")"),
                new Transformation(3, value -> "third(" + value + ")")));
    }

    @Test
    public void testMultipleDecoratorSortedByPriorityShouldSkip()
    {
        assertFinalValue("test", "second(first(test))", List.of(
                new Transformation(2, value -> "second(" + value + ")"),
                new Transformation(0, value -> "skipped(" + value + ")"),
                new Transformation(1, value -> "first(" + value + ")")));

        assertFinalValue("test2", "third(second(first(test2)))", List.of(
                new Transformation(2, value -> "second(" + value + ")"),
                new Transformation(1, value -> "first(" + value + ")"),
                new Transformation(0, value -> "skipped(" + value + ")"),
                new Transformation(3, value -> "third(" + value + ")")));
    }

    @Test
    public void testMisbehavingDecorator()
    {
        assertThatThrownBy(() -> assertFinalValue("test", "second(first(test))", List.of(
                new Transformation(2, value -> null))))
                .hasMessageContaining("NullPointerException: decorator f(x) = null returned null value");
    }

    private static void assertFinalValue(String initialValue, String finalValue, List<Transformation> transformations)
    {
        assertThat(finalValue(initialValue, transformations))
                .isEqualTo(finalValue);
    }

    private static String finalValue(String initial, List<Transformation> transformations)
    {
        Injector injector = new Bootstrap(new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                binder
                        .bind(String.class)
                        .annotatedWith(InitialValue.class)
                        .toInstance(initial);

                DecoratorBinder<String> decoratorBinder = newDecoratorBinder(binder, String.class, InitialValue.class);
                for (Transformation transformation : transformations) {
                    decoratorBinder.addInstanceBinding(transformation);
                }
            }
        })
                .initialize();

        return injector.getInstance(String.class);
    }

    static class Transformation
            implements Decorator<String>
    {
        private final Function<String, String> transformation;
        private final int priority;

        public Transformation(int priority, Function<String, String> transformation)
        {
            this.priority = priority;
            this.transformation = requireNonNull(transformation, "transformation is null");
        }

        @Override
        public String apply(String instance)
        {
            return transformation.apply(instance);
        }

        @Override
        public int priority()
        {
            return priority;
        }

        @Override
        public boolean appliesTo(String instance)
        {
            return priority > 0;
        }

        @Override
        public String toString()
        {
            return "f(x) = " + transformation.apply("x");
        }
    }

    @BindingAnnotation
    @Retention(RUNTIME)
    public @interface InitialValue {}
}
