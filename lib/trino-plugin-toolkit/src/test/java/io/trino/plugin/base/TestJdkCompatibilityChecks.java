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
package io.trino.plugin.base;

import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Binder;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Module;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.base.JdkCompatibilityChecks.verifyConnectorAccessOpened;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJdkCompatibilityChecks
{
    @Test
    public void shouldThrowWhenAccessIsNotGranted()
    {
        ThrowableSettableMock errorSink = new ThrowableSettableMock();
        new JdkCompatibilityChecks(List.of()).verifyAccessOpened(errorSink, "Connector 'snowflake'", ImmutableMultimap.of(
                "java.base", "java.nio"));

        assertThat(errorSink.getThrowable())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Connector 'snowflake' requires additional JVM argument(s). Please add the following to the JVM configuration: '--add-opens=java.base/java.nio=ALL-UNNAMED'");
    }

    @Test
    public void shouldThrowWhenOneOfAccessGrantsIsMissing()
    {
        ThrowableSettableMock errorSink = new ThrowableSettableMock();
        new JdkCompatibilityChecks(List.of("--add-opens", "java.base/java.nio=ALL-UNNAMED")).verifyAccessOpened(errorSink, "Connector 'snowflake'", ImmutableMultimap.of(
                "java.base", "java.nio",
                "java.base", "java.lang"));

        assertThat(errorSink.getThrowable())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Connector 'snowflake' requires additional JVM argument(s). Please add the following to the JVM configuration: '--add-opens=java.base/java.lang=ALL-UNNAMED'");
    }

    @Test
    public void shouldSetErrorOnBinder()
    {
        assertThatThrownBy(() -> Guice.createInjector(new TestingModule()))
                .isInstanceOf(CreationException.class)
                .hasMessageContaining("Unable to create injector, see the following errors:")
                .hasMessageContaining("1) Connector 'Testing' requires additional JVM argument(s). Please add the following to the JVM configuration: '--add-opens=java.base/java.non.existing=ALL-UNNAMED --add-opens=java.base/java.this.should.fail=ALL-UNNAMED'");
    }

    @Test
    public void shouldNotThrowWhenAllAccessGrantsArePresent()
    {
        ThrowableSettableMock errorSink = new ThrowableSettableMock();
        new JdkCompatibilityChecks(List.of("--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens java.base/java.lang=ALL-UNNAMED")).verifyAccessOpened(errorSink, "Connector 'snowflake'", ImmutableMultimap.of(
                "java.base", "java.nio",
                "java.base", "java.lang"));

        assertThat(errorSink.getThrowable()).isNull();
    }

    @Test
    public void shouldThrowWhenUnsafeIsNotAllowed()
    {
        ThrowableSettableMock errorSink = new ThrowableSettableMock();
        new JdkCompatibilityChecks(List.of()).verifyUnsafeAllowed(errorSink, "Connector 'bigquery'");

        assertThat(errorSink.getThrowable())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Connector 'bigquery' requires additional JVM argument(s). Please add the following to the JVM configuration: '--sun-misc-unsafe-memory-access=allow'");
    }

    @Test
    public void shouldNotThrowWhenUnsafeIsAllowed()
    {
        ThrowableSettableMock errorSink = new ThrowableSettableMock();
        new JdkCompatibilityChecks(List.of("--sun-misc-unsafe-memory-access=allow")).verifyUnsafeAllowed(errorSink, "Connector 'bigquery'");

        assertThat(errorSink.getThrowable())
                .isNull();
    }

    private static class TestingModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            verifyConnectorAccessOpened(binder, "Testing", ImmutableMultimap.of(
                    "java.base", "java.non.existing",
                    "java.base", "java.this.should.fail"));
        }
    }

    private static class ThrowableSettableMock
            implements JdkCompatibilityChecks.ThrowableSettable
    {
        private Throwable throwable;

        @Override
        public void setThrowable(Throwable throwable)
        {
            this.throwable = requireNonNull(throwable, "throwable is null");
        }

        public Throwable getThrowable()
        {
            return throwable;
        }
    }
}
