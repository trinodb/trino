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

import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.sql.analyzer.SemanticException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.util.CheckReturnValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public final class SemanticExceptionAssert
        extends AbstractThrowableAssert<SemanticExceptionAssert, SemanticException>
{
    @CheckReturnValue
    public static SemanticExceptionAssert assertSemanticExceptionThrownBy(ThrowingCallable throwingCallable)
    {
        Throwable throwable = catchThrowable(throwingCallable);
        if (throwable == null) {
            failBecauseExceptionWasNotThrown(SemanticException.class);
        }
        assertThat(throwable).isInstanceOf(SemanticException.class);
        return new SemanticExceptionAssert((SemanticException) throwable);
    }

    private SemanticExceptionAssert(SemanticException actual)
    {
        super(actual, SemanticExceptionAssert.class);
    }

    public SemanticExceptionAssert hasErrorCode(SemanticErrorCode errorCode)
    {
        try {
            assertThat(actual.getCode()).isEqualTo(errorCode);
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }
}
