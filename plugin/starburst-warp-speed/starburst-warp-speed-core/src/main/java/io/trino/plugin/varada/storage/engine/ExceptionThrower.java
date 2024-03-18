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
package io.trino.plugin.varada.storage.engine;

import io.trino.plugin.warp.gen.errorcodes.ErrorCodes;
import io.trino.spi.TrinoException;

import java.util.function.Consumer;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_ERROR;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_READ_OUT_OF_BOUNDS;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_UNRECOVERABLE_ERROR;

public interface ExceptionThrower
{
    static boolean isNativeException(TrinoException te)
    {
        int code = te.getErrorCode().getCode();
        return code == VARADA_NATIVE_ERROR.toErrorCode().getCode() || code == VARADA_NATIVE_UNRECOVERABLE_ERROR.toErrorCode().getCode() || code == VARADA_NATIVE_READ_OUT_OF_BOUNDS.toErrorCode().getCode();
    }

    void throwException(int code, Object[] params);

    void addExceptionConsumer(Consumer<ErrorCodes> errorCodesConsumer);
}
