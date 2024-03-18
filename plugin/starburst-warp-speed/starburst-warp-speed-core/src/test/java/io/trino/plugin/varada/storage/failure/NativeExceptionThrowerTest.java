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
package io.trino.plugin.varada.storage.failure;

import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.gen.errorcodes.ErrorCodes;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_ERROR;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_READ_OUT_OF_BOUNDS;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_UNRECOVERABLE_ERROR;
import static org.assertj.core.api.Assertions.assertThat;

public class NativeExceptionThrowerTest
{
    @Test
    public void testValidateNonDuplicateErrorCodes()
    {
        Map<Integer, ErrorCodes> codes = new HashMap<>();
        for (ErrorCodes errorCode : ErrorCodes.values()) {
            codes.put(errorCode.getCode(), errorCode);
        }
        assertThat(codes.size()).isEqualTo(ErrorCodes.values().length);
    }

    @Test
    public void testIsNativeException()
    {
        assertThat(ExceptionThrower.isNativeException(new TrinoException(VARADA_NATIVE_UNRECOVERABLE_ERROR, "msg"))).isTrue();
        assertThat(ExceptionThrower.isNativeException(new TrinoException(VARADA_NATIVE_ERROR, "msg"))).isTrue();
        assertThat(ExceptionThrower.isNativeException(new TrinoException(VARADA_NATIVE_READ_OUT_OF_BOUNDS, "msg"))).isTrue();
        Arrays.stream(VaradaErrorCode.values())
                .filter(varadaErrorCode -> !(VARADA_NATIVE_UNRECOVERABLE_ERROR.equals(varadaErrorCode) || VARADA_NATIVE_ERROR.equals(varadaErrorCode) || VARADA_NATIVE_READ_OUT_OF_BOUNDS.equals(varadaErrorCode)))
                .forEach(varadaErrorCode -> assertThat(ExceptionThrower.isNativeException(new TrinoException(varadaErrorCode, "msg"))).isFalse());
    }
}
