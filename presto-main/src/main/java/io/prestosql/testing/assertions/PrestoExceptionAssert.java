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
package io.prestosql.testing.assertions;

import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.PrestoException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.util.CheckReturnValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public final class PrestoExceptionAssert
        extends AbstractThrowableAssert<PrestoExceptionAssert, PrestoException>
{
    @CheckReturnValue
    public static PrestoExceptionAssert assertPrestoExceptionThrownBy(ThrowingCallable throwingCallable)
    {
        Throwable throwable = catchThrowable(throwingCallable);
        if (throwable == null) {
            failBecauseExceptionWasNotThrown(PrestoException.class);
        }
        assertThat(throwable).isInstanceOf(PrestoException.class);
        return new PrestoExceptionAssert((PrestoException) throwable);
    }

    private PrestoExceptionAssert(PrestoException actual)
    {
        super(actual, PrestoExceptionAssert.class);
    }

    public PrestoExceptionAssert hasErrorCode(ErrorCodeSupplier errorCodeSupplier)
    {
        try {
            assertThat(actual.getErrorCode()).isEqualTo(errorCodeSupplier.toErrorCode());
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }

    public PrestoExceptionAssert hasLocation(int lineNumber, int columnNumber)
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
