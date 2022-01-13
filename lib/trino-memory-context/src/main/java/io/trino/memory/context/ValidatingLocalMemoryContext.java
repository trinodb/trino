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
package io.trino.memory.context;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import static java.util.Objects.requireNonNull;

public class ValidatingLocalMemoryContext
        implements LocalMemoryContext
{
    private static final Logger log = Logger.get(ValidatingLocalMemoryContext.class);

    private final LocalMemoryContext delegate;
    private final String allocationTag;
    private final MemoryAllocationValidator memoryValidator;

    public ValidatingLocalMemoryContext(LocalMemoryContext delegate, String allocationTag, MemoryAllocationValidator memoryValidator)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.allocationTag = requireNonNull(allocationTag, "allocationTag is null");
        this.memoryValidator = requireNonNull(memoryValidator, "memoryValidator is null");
    }

    @Override
    public long getBytes()
    {
        return delegate.getBytes();
    }

    @Override
    public ListenableFuture<Void> setBytes(long bytes)
    {
        long delta = bytes - delegate.getBytes();

        // first consult validator if allocation is possible
        memoryValidator.reserveMemory(allocationTag, delta);

        // update the parent before updating usedBytes as it may throw a runtime exception (e.g., ExceededMemoryLimitException)
        try {
            // do actual allocation
            return delegate.setBytes(bytes);
        }
        catch (Exception e) {
            revertReservationInValidatorSuppressing(allocationTag, delta, e);
            throw e;
        }
    }

    @Override
    public boolean trySetBytes(long bytes)
    {
        long delta = bytes - delegate.getBytes();

        if (!memoryValidator.tryReserveMemory(allocationTag, delta)) {
            return false;
        }

        try {
            if (delegate.trySetBytes(bytes)) {
                return true;
            }
        }
        catch (Exception e) {
            revertReservationInValidatorSuppressing(allocationTag, delta, e);
            throw e;
        }

        revertReservationInValidator(allocationTag, delta);
        return false;
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    private void revertReservationInValidatorSuppressing(String allocationTag, long delta, Exception revertCause)
    {
        try {
            revertReservationInValidator(allocationTag, delta);
        }
        catch (Exception suppressed) {
            log.warn(suppressed, "Could not rollback memory reservation within allocation validator");
            if (suppressed != revertCause) {
                revertCause.addSuppressed(suppressed);
            }
        }
    }

    private void revertReservationInValidator(String allocationTag, long delta)
    {
        memoryValidator.reserveMemory(allocationTag, -delta);
    }
}
