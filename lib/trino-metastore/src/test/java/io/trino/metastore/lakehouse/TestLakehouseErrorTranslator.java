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
package io.trino.metastore.lakehouse;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.assertj.core.api.Assertions.assertThat;

class TestLakehouseErrorTranslator
{
    @Test
    void reportsAMissingObjectSeparatelyBecauseItIsNotAFailure()
    {
        assertThat(LakehouseErrorTranslator.isNotFound(failure(StatusCode.Code.NOT_FOUND))).isTrue();
        assertThat(LakehouseErrorTranslator.isNotFound(failure(StatusCode.Code.PERMISSION_DENIED))).isFalse();
    }

    @Test
    void reportsAMissingDatabaseOrCatalogSeparately()
    {
        assertThat(LakehouseErrorTranslator.isMissingParent(failure(StatusCode.Code.FAILED_PRECONDITION))).isTrue();
        assertThat(LakehouseErrorTranslator.isMissingParent(failure(StatusCode.Code.NOT_FOUND))).isFalse();
    }

    @Test
    void keepsAuthorizationFailuresDistinct()
    {
        assertCode(StatusCode.Code.PERMISSION_DENIED, PERMISSION_DENIED);
        assertCode(StatusCode.Code.UNAUTHENTICATED, PERMISSION_DENIED);
    }

    @Test
    void mapsTheRemainingFailuresThatTrinoDistinguishes()
    {
        assertCode(StatusCode.Code.ALREADY_EXISTS, ALREADY_EXISTS);
        assertCode(StatusCode.Code.INVALID_ARGUMENT, INVALID_ARGUMENTS);
        assertCode(StatusCode.Code.UNAVAILABLE, GENERIC_INTERNAL_ERROR);
        assertCode(StatusCode.Code.INTERNAL, GENERIC_INTERNAL_ERROR);
    }

    @Test
    void namesTheOperationInEveryMessage()
    {
        assertThat(LakehouseErrorTranslator.translate("reading table sales.orders", failure(StatusCode.Code.INTERNAL)))
                .hasMessageContaining("reading table sales.orders");
    }

    @Test
    void appendsTheReasonThatLakehouseGave()
    {
        assertThat(LakehouseErrorTranslator.translate("reading table sales.orders", failure(StatusCode.Code.INTERNAL)))
                .hasMessage("reading table sales.orders: Lakehouse request failed: boom");
        assertThat(LakehouseErrorTranslator.translate("reading table sales.orders", failure(StatusCode.Code.PERMISSION_DENIED)))
                .hasMessage("reading table sales.orders: Lakehouse denied the request: boom");
    }

    @Test
    void reportsAnExcludedOperationAsUnsupported()
    {
        TrinoException thrown = LakehouseErrorTranslator.notSupported("creating tables");

        assertThat(thrown.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
        assertThat(thrown).hasMessageContaining("creating tables");
    }

    private static void assertCode(StatusCode.Code code, ErrorCodeSupplier expected)
    {
        assertThat(LakehouseErrorTranslator.translate("reading", failure(code)).getErrorCode())
                .isEqualTo(expected.toErrorCode());
    }

    private static ApiException failure(StatusCode.Code code)
    {
        return ApiExceptionFactory.createException(new RuntimeException("boom"), new TestingStatusCode(code), false);
    }

    private record TestingStatusCode(Code getCode)
            implements StatusCode
    {
        @Override
        public Object getTransportCode()
        {
            return null;
        }
    }
}
