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

import io.trino.execution.ExecutionFailureInfo;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.testing.QueryFailedException;
import io.trino.util.Failures;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoExceptionAssert
{
    @Test
    public void testCatchTrinoException()
    {
        assertTrinoExceptionThrownBy(() -> {
            throw new TrinoException(NOT_SUPPORTED, "test exception");
        })
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("test exception");
    }

    @Test
    public void testCatchRemoteTrinoException()
    {
        ExecutionFailureInfo failureInfo = Failures.toFailure(new TrinoException(NOT_SUPPORTED, "remote exception"));
        assertTrinoExceptionThrownBy(() -> {
            throw new QueryFailedException(new QueryId("test"), failureInfo.getMessage(), failureInfo.toException());
        })
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("remote exception");
    }

    @Test
    public void testRejectRuntimeException()
    {
        assertThatThrownBy(() -> assertTrinoExceptionThrownBy(() -> {
            throw new RuntimeException("not a trino exception");
        }))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected TrinoException or wrapper, but got: java.lang.RuntimeException java.lang.RuntimeException: not a trino exception");
    }

    @Test
    public void testRejectRemoteRuntimeException()
    {
        ExecutionFailureInfo failureInfo = Failures.toFailure(new RuntimeException("not a trino exception"));
        assertThatThrownBy(() -> assertTrinoExceptionThrownBy(() -> {
            throw new QueryFailedException(new QueryId("test"), failureInfo.getMessage(), failureInfo.toException());
        }))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected TrinoException or wrapper, but got: io.trino.testing.QueryFailedException io.trino.testing.QueryFailedException: not a trino exception");
    }

    @Test
    public void testRejectWeirdRemoteTrinoException()
    {
        TrinoException trinoException = new TrinoException(NOT_SUPPORTED, "test exception");
        assertThatThrownBy(() -> assertTrinoExceptionThrownBy(() -> {
            throw new QueryFailedException(new QueryId("test"), trinoException.getMessage(), trinoException);
        }))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected TrinoException or wrapper, but got: io.trino.testing.QueryFailedException io.trino.testing.QueryFailedException: test exception");
    }
}
