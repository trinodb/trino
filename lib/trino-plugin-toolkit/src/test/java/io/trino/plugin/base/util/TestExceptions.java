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
package io.trino.plugin.base.util;

import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.base.util.Exceptions.findErrorCode;
import static io.trino.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static org.assertj.core.api.Assertions.assertThat;

final class TestExceptions
{
    @Test
    void testFindErrorCodeWithTrinoException()
    {
        Throwable exception = new TrinoException(TOO_MANY_REQUESTS_FAILED, "fake exception");
        Optional<ErrorCodeSupplier> errorCodeSupplier = findErrorCode(exception);
        assertThat(errorCodeSupplier.orElseThrow().toErrorCode()).isEqualTo(TOO_MANY_REQUESTS_FAILED.toErrorCode());
    }

    @Test
    void testFindErrorCodeWithRuntimeException()
    {
        Throwable exception = new RuntimeException();
        Optional<ErrorCodeSupplier> errorCodeSupplier = findErrorCode(exception);
        assertThat(errorCodeSupplier).isEmpty();
    }

    @Test
    void testFindErrorCodeWithExceptionStack()
    {
        Throwable exception1 = new TrinoException(USER_CANCELED, "fake exception");
        Throwable exception2 = new RuntimeException(exception1);
        assertThat(findErrorCode(exception2).orElseThrow().toErrorCode()).isEqualTo(USER_CANCELED.toErrorCode());

        Throwable exception3 = new RuntimeException(exception2);
        assertThat(findErrorCode(exception3).orElseThrow().toErrorCode()).isEqualTo(USER_CANCELED.toErrorCode());
    }
}
