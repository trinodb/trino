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
package io.trino.plugin.varada.storage.engine.nativeimpl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.gen.errorcodes.ErrorCodes;
import io.trino.plugin.warp.gen.stats.VaradaStatsExceptionThrower;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_ERROR;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_READ_OUT_OF_BOUNDS;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_UNRECOVERABLE_ERROR;
import static java.lang.String.format;

@Singleton
public class NativeExceptionThrower
        implements ExceptionThrower
{
    private static final Logger logger = Logger.get(NativeExceptionThrower.class);
    private final VaradaStatsExceptionThrower varadaStatsExceptionThrower;
    private final Map<Integer, ErrorCodes> errorCodesMap = new HashMap<>();
    private final List<Consumer<ErrorCodes>> errorCodesConsumers = new ArrayList<>();

    @Inject
    public NativeExceptionThrower(MetricsManager metricsManager)
    {
        for (ErrorCodes errorCode : ErrorCodes.values()) {
            errorCodesMap.put(errorCode.getCode(), errorCode);
        }
        varadaStatsExceptionThrower = VaradaStatsExceptionThrower.create("exception-thrower");
        metricsManager.registerMetric(this.varadaStatsExceptionThrower);
        nativeInit();
    }

    @Override
    public void throwException(int code, Object[] params)
    {
        ErrorCodes errorCode = errorCodesMap.get(code);
        if (errorCode.getUnrecoverable()) {
            varadaStatsExceptionThrower.incnonRecoverable();
        }
        else {
            varadaStatsExceptionThrower.increcoverable();
        }

        logger.debug("calling errorCodesConsumers[%d] with %s", errorCodesConsumers.size(), errorCode);
        errorCodesConsumers.forEach(errorCodesConsumer -> errorCodesConsumer.accept(errorCode));

        String errorMessage = (params == null) ? errorCode.getMessage() : format(errorCode.getMessage(), params);
        String formatMessage = errorMessage + " (" + code + ")";
        VaradaErrorCode trinoExceptionErrorCode = VARADA_NATIVE_ERROR;
        if (errorCode.getUnrecoverable()) {
            trinoExceptionErrorCode = VARADA_NATIVE_UNRECOVERABLE_ERROR;
        }
        else if (errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_READ_OUT_OF_BOUNDS)) {
            trinoExceptionErrorCode = VARADA_NATIVE_READ_OUT_OF_BOUNDS;
        }
        throw new TrinoException(trinoExceptionErrorCode, formatMessage);
    }

    @Override
    public void addExceptionConsumer(Consumer<ErrorCodes> errorCodesConsumer)
    {
        errorCodesConsumers.add(errorCodesConsumer);
    }

    private native void nativeInit();
}
