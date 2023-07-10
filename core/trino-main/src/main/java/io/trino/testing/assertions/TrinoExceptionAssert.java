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
package io.trino.testing.assertions;

import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.util.CheckReturnValue;

import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public final class TrinoExceptionAssert
        extends AbstractThrowableAssert<TrinoExceptionAssert, TrinoException>
{
    @CheckReturnValue
    public static TrinoExceptionAssert assertTrinoExceptionThrownBy(ThrowingCallable throwingCallable)
    {
        Throwable throwable = catchThrowable(throwingCallable);
        if (throwable == null) {
            failBecauseExceptionWasNotThrown(TrinoException.class);
        }
        assertThat(throwable).isInstanceOf(TrinoException.class);
        return new TrinoExceptionAssert((TrinoException) throwable);
    }

    private TrinoExceptionAssert(TrinoException actual)
    {
        super(actual, TrinoExceptionAssert.class);
    }

    public TrinoExceptionAssert hasErrorCode(ErrorCodeSupplier... errorCodeSupplier)
    {
        try {
            assertThat(actual.getErrorCode()).isIn(
                    Stream.of(errorCodeSupplier)
                            .map(ErrorCodeSupplier::toErrorCode)
                            .collect(toSet()));
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }

    public TrinoExceptionAssert hasLocation(int lineNumber, int columnNumber)
    {
        try {
            assertThat(actual.getLocation()).isPresent();
            assertThat(actual.getLocation().get())
                    .matches(location -> location.getColumnNumber() == columnNumber && location.getLineNumber() == lineNumber);
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }
}
