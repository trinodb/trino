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
package io.trino.util;

import io.trino.execution.ExecutionFailureInfo;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static io.trino.util.Failures.toFailure;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFailures
{
    @Test
    public void testToFailureLoop()
    {
        Throwable exception1 = new TrinoException(TOO_MANY_REQUESTS_FAILED, "fake exception 1");
        Throwable exception2 = new RuntimeException("fake exception 2", exception1);
        exception1.addSuppressed(exception2);

        // add exception 1 --> add suppress (exception 2) --> add cause (exception 1)
        ExecutionFailureInfo failure = toFailure(exception1);
        assertThat(failure.getMessage()).isEqualTo("fake exception 1");
        assertThat(failure.getCause()).isNull();
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed().get(0).getMessage()).isEqualTo("fake exception 2");
        assertThat(failure.getErrorCode()).isEqualTo(TOO_MANY_REQUESTS_FAILED.toErrorCode());

        // add exception 2 --> add cause (exception 2) --> add suppress (exception 1)
        failure = toFailure(exception2);
        assertThat(failure.getMessage()).isEqualTo("fake exception 2");
        assertThat(failure.getCause()).isNotNull();
        assertThat(failure.getCause().getMessage()).isEqualTo("fake exception 1");
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(failure.getErrorCode()).isEqualTo(TOO_MANY_REQUESTS_FAILED.toErrorCode());

        // add exception 1 --> add suppress (exception 2) --> add suppress (exception 1)
        exception1 = new TrinoException(TOO_MANY_REQUESTS_FAILED, "fake exception 1");
        exception2 = new RuntimeException("fake exception 2");
        exception1.addSuppressed(exception2);
        exception2.addSuppressed(exception1);
        failure = toFailure(exception1);
        assertThat(failure.getMessage()).isEqualTo("fake exception 1");
        assertThat(failure.getCause()).isNull();
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed().get(0).getMessage()).isEqualTo("fake exception 2");
        assertThat(failure.getErrorCode()).isEqualTo(TOO_MANY_REQUESTS_FAILED.toErrorCode());

        // add exception 2 --> add cause (exception 1) --> add cause (exception 2)
        exception1 = new RuntimeException("fake exception 1");
        exception2 = new RuntimeException("fake exception 2", exception1);
        exception1.initCause(exception2);
        failure = toFailure(exception2);
        assertThat(failure.getMessage()).isEqualTo("fake exception 2");
        assertThat(failure.getCause()).isNotNull();
        assertThat(failure.getCause().getMessage()).isEqualTo("fake exception 1");
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(failure.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
    }
}
