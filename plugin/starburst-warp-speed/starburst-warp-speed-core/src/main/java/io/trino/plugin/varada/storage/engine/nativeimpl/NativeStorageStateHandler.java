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
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.gen.errorcodes.ErrorCodes;
import io.varada.log.ShapingLogger;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageStateHandler
{
    private final NativeConfiguration nativeConfiguration;
    private final ShapingLogger shapingLogger;

    boolean storageDisablePermanently;
    boolean storageDisableTemporarily;
    long storageTemporaryExceptionTimestamp;
    long storageTemporaryExceptionNumTries;
    long storageTemporaryExceptionExpiryTimestamp;

    @Inject
    public NativeStorageStateHandler(
            NativeConfiguration nativeConfiguration,
            ExceptionThrower exceptionThrower,
            GlobalConfiguration globalConfiguration)
    {
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
        exceptionThrower.addExceptionConsumer(this::handleErrorCode);
        shapingLogger = ShapingLogger.getInstance(
                Logger.get(NativeStorageStateHandler.class),
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    public boolean isStorageAvailable()
    {
        if (isStorageDisabledPermanently()) {
            return false;
        }
        if (isStorageDisabledTemporarily()) {
            long configuredExpiryDurationMillis = nativeConfiguration.getStorageTemporaryExceptionExpiryDuration().toMillis();
            long currentDurationMillis = System.currentTimeMillis() - storageTemporaryExceptionTimestamp;

            // reset storage temp params in case timeout expiry passed
            if (currentDurationMillis > configuredExpiryDurationMillis) {
                synchronized (this) {
                    resetTempState();
                }
            }
            else {
                return false;
            }
        }
        return true;
    }

    public synchronized void handleErrorCode(ErrorCodes errorCode)
    {
        shapingLogger.info("handleErrorCode:: %s", errorCode);

        if (errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_PERMANENT_ERROR)) {
            storageDisablePermanently = true;
            shapingLogger.warn("set storage permanent state due to %s", errorCode);
        }
        else if (errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR) || errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_TIMEOUT_ERROR)) {
            long currentTimeMillis = System.currentTimeMillis();

            // initialize on first temp error
            if (!storageDisableTemporarily) {
                storageDisableTemporarily = true;
                storageTemporaryExceptionExpiryTimestamp = currentTimeMillis;
                shapingLogger.warn("set storage temporary state due to %s", errorCode);
            }

            storageTemporaryExceptionNumTries++;
            storageTemporaryExceptionTimestamp = currentTimeMillis;
            shapingLogger.warn("set storage temporary tries [%d]", storageTemporaryExceptionNumTries);

            // mark as permanent in case too many temp errors
            if (storageTemporaryExceptionNumTries >= nativeConfiguration.getStorageTemporaryExceptionNumTries()) {
                storageDisablePermanently = true;
                shapingLogger.warn("set storage permanent state due to too many temporary errors - %s", errorCode);
            }
        }
    }

    private void resetTempState()
    {
        storageDisableTemporarily = false;
        storageTemporaryExceptionNumTries = 0;
        storageTemporaryExceptionTimestamp = 0L;
        storageTemporaryExceptionExpiryTimestamp = 0L;
    }

    public boolean isStorageDisabledPermanently()
    {
        return storageDisablePermanently;
    }

    public boolean isStorageDisabledTemporarily()
    {
        return storageDisableTemporarily;
    }

    public synchronized void setStorageDisableState(boolean disablePermanently, boolean disableTemporarily)
    {
        storageDisablePermanently = disablePermanently;
        storageDisableTemporarily = disableTemporarily;
        if (!storageDisableTemporarily) {
            shapingLogger.info("reset storage temporary state due to expiry duration");
            resetTempState();
        }
    }
}
