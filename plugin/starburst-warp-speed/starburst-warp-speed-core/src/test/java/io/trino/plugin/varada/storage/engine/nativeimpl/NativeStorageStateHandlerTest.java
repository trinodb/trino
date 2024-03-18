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

import io.airlift.units.Duration;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.warp.gen.errorcodes.ErrorCodes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.gen.errorcodes.ErrorCodes.ENV_EXCEPTION_STORAGE_PERMANENT_ERROR;
import static io.trino.plugin.warp.gen.errorcodes.ErrorCodes.ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR;
import static io.trino.plugin.warp.gen.errorcodes.ErrorCodes.ENV_EXCEPTION_STORAGE_TIMEOUT_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class NativeStorageStateHandlerTest
{
    private final Map<Integer, ErrorCodes> tempErrorCodes = Map.of(
            0, ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR,
            1, ENV_EXCEPTION_STORAGE_TIMEOUT_ERROR);
    private NativeConfiguration nativeConfiguration;
    private NativeStorageStateHandler handler;
    private final Random random = new Random();

    @BeforeEach
    public void beforeEach()
    {
        nativeConfiguration = new NativeConfiguration();
        nativeConfiguration.setStorageTemporaryExceptionDuration(new Duration(1000, TimeUnit.MILLISECONDS));
        nativeConfiguration.setStorageTemporaryExceptionExpiryDuration(new Duration(10000, TimeUnit.MILLISECONDS));

        NativeExceptionThrower nativeExceptionThrower = mock(NativeExceptionThrower.class);
        handler = new NativeStorageStateHandler(nativeConfiguration, nativeExceptionThrower, new GlobalConfiguration());
    }

    @Test
    public void testStorageStateUninitialized()
    {
        assertThat(handler.storageDisablePermanently).isFalse();
        assertThat(handler.storageDisableTemporarily).isFalse();
        assertThat(handler.storageTemporaryExceptionNumTries).isZero();
        assertThat(handler.storageTemporaryExceptionTimestamp).isZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isZero();

        assertThat(handler.isStorageAvailable()).isTrue();
    }

    @Test
    public void testStorageStateNonRelatedErrorCodes()
    {
        Arrays.stream(ErrorCodes.values())
                .filter(errorCode -> !(ENV_EXCEPTION_STORAGE_PERMANENT_ERROR.equals(errorCode) || tempErrorCodes.containsValue(errorCode)))
                .forEach(errorCode -> {
                    handler.handleErrorCode(errorCode);
                    assertThat(handler.isStorageAvailable()).isTrue();
                });

        assertThat(handler.storageDisablePermanently).isFalse();
        assertThat(handler.storageDisableTemporarily).isFalse();
        assertThat(handler.storageTemporaryExceptionNumTries).isZero();
        assertThat(handler.storageTemporaryExceptionTimestamp).isZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isZero();
    }

    @Test
    public void testStorageStateRegularPermanentErrorCode()
    {
        handler.handleErrorCode(ENV_EXCEPTION_STORAGE_PERMANENT_ERROR);

        assertThat(handler.isStorageAvailable()).isFalse();

        assertThat(handler.storageDisablePermanently).isTrue();
        assertThat(handler.storageDisableTemporarily).isFalse();
        assertThat(handler.storageTemporaryExceptionNumTries).isZero();
        assertThat(handler.storageTemporaryExceptionTimestamp).isZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isZero();
    }

    @Test
    public void testStorageStateTempErrorCodesNoTimeout()
    {
        int times = nativeConfiguration.getStorageTemporaryExceptionNumTries();

        IntStream.range(1, times)
                .forEach(i -> {
                    handler.handleErrorCode(tempErrorCodes.get(Math.abs(random.nextInt(Integer.MAX_VALUE)) % 2));
                    assertThat(handler.isStorageAvailable()).isFalse();

                    assertThat(handler.storageDisablePermanently).isFalse();
                    assertThat(handler.storageDisableTemporarily).isTrue();
                    assertThat(handler.storageTemporaryExceptionNumTries).isGreaterThanOrEqualTo(1);
                    assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
                    assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();
                });
        assertThat(handler.isStorageAvailable()).isFalse();

        assertThat(handler.storageDisablePermanently).isFalse();
        assertThat(handler.storageDisableTemporarily).isTrue();
        assertThat(handler.storageTemporaryExceptionNumTries).isEqualTo(2);
        assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();
    }

    @Test
    public void testStorageStateTempErrorCodesSmallTimeout()
    {
        int times = nativeConfiguration.getStorageTemporaryExceptionNumTries();

        IntStream.range(1, times)
                .forEach(i -> {
                    handler.handleErrorCode(tempErrorCodes.get(Math.abs(random.nextInt(Integer.MAX_VALUE)) % 2));
                    assertThat(handler.isStorageAvailable()).isFalse();

                    assertThat(handler.storageDisablePermanently).isFalse();
                    assertThat(handler.storageDisableTemporarily).isTrue();
                    assertThat(handler.storageTemporaryExceptionNumTries).isGreaterThanOrEqualTo(1);
                    assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
                    assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();

                    try {
                        Thread.sleep(2);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        assertThat(handler.isStorageAvailable()).isFalse();

        assertThat(handler.storageDisablePermanently).isFalse();
        assertThat(handler.storageDisableTemporarily).isTrue();
        assertThat(handler.storageTemporaryExceptionNumTries).isEqualTo(2);
        assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();
    }

    @Test
    public void testStorageStateTempErrorCodesWithTimeoutToPermanent()
    {
        nativeConfiguration.setStorageTemporaryExceptionDuration(new Duration(10, TimeUnit.MILLISECONDS));
        int times = nativeConfiguration.getStorageTemporaryExceptionNumTries();
        long sleepTime = nativeConfiguration.getStorageTemporaryExceptionDuration().toMillis() + 1;

        IntStream.range(1, times)
                .forEach(i -> {
                    handler.handleErrorCode(tempErrorCodes.get(Math.abs(random.nextInt(Integer.MAX_VALUE)) % 2));
                    assertThat(handler.isStorageAvailable()).isFalse();

                    assertThat(handler.storageDisablePermanently).isFalse();
                    assertThat(handler.storageDisableTemporarily).isTrue();
                    assertThat(handler.storageTemporaryExceptionNumTries).isEqualTo(i);
                    assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
                    assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();

                    try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        handler.handleErrorCode(tempErrorCodes.get(Math.abs(random.nextInt(Integer.MAX_VALUE)) % 2));
        assertThat(handler.isStorageAvailable()).isFalse();

        assertThat(handler.storageDisablePermanently).isTrue();
        assertThat(handler.storageDisableTemporarily).isTrue();
        assertThat(handler.storageTemporaryExceptionNumTries).isEqualTo(nativeConfiguration.getStorageTemporaryExceptionNumTries());
        assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();
    }

    @Test
    public void testStorageStateTempErrorCodesReset()
            throws InterruptedException
    {
        nativeConfiguration.setStorageTemporaryExceptionDuration(new Duration(10, TimeUnit.MILLISECONDS));
        nativeConfiguration.setStorageTemporaryExceptionExpiryDuration(new Duration(100, TimeUnit.MILLISECONDS));
        int times = nativeConfiguration.getStorageTemporaryExceptionNumTries();
        long sleepTime = nativeConfiguration.getStorageTemporaryExceptionDuration().toMillis() + 1;

        IntStream.range(1, times)
                .forEach(i -> {
                    handler.handleErrorCode(tempErrorCodes.get(Math.abs(random.nextInt(Integer.MAX_VALUE)) % 2));
                    assertThat(handler.isStorageAvailable()).isFalse();

                    assertThat(handler.storageDisablePermanently).isFalse();
                    assertThat(handler.storageDisableTemporarily).isTrue();
                    assertThat(handler.storageTemporaryExceptionNumTries).isGreaterThanOrEqualTo(i);
                    assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
                    assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();

                    try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        assertThat(handler.isStorageAvailable()).isFalse();

        assertThat(handler.storageDisablePermanently).isFalse();
        assertThat(handler.storageDisableTemporarily).isTrue();
        assertThat(handler.storageTemporaryExceptionNumTries).isLessThan(nativeConfiguration.getStorageTemporaryExceptionNumTries());
        assertThat(handler.storageTemporaryExceptionTimestamp).isNotZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isNotZero();

        Thread.sleep(nativeConfiguration.getStorageTemporaryExceptionExpiryDuration().toMillis() + 1);

        assertThat(handler.isStorageAvailable()).isTrue();

        assertThat(handler.storageDisablePermanently).isFalse();
        assertThat(handler.storageDisableTemporarily).isFalse();
        assertThat(handler.storageTemporaryExceptionNumTries).isZero();
        assertThat(handler.storageTemporaryExceptionTimestamp).isZero();
        assertThat(handler.storageTemporaryExceptionExpiryTimestamp).isZero();
    }
}
