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
package io.trino.plugin.varada.dispatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.trino.spi.TrinoException;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_SETUP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;

public class CachingPlugin
{
    protected Module storageEngineModule;
    protected Module cloudVendorModule;

    private static void verifyTypeSizes()
    {
        if (BIGINT.getFixedSize() != Long.BYTES) {
            throw new TrinoException(VARADA_SETUP, "BIGINT size is not equal to Long size");
        }
        if (BOOLEAN.getFixedSize() != Byte.BYTES) {
            throw new TrinoException(VARADA_SETUP, "BOOLEAN size is not equal to Byte size");
        }
        if (DATE.getFixedSize() != Integer.BYTES) {
            throw new TrinoException(VARADA_SETUP, "DATE size is not equal to Integer size");
        }
        if (REAL.getFixedSize() != Integer.BYTES) {
            throw new TrinoException(VARADA_SETUP, "REAL size is not equal to Integer size");
        }
        if (DOUBLE.getFixedSize() != Double.BYTES) {
            throw new TrinoException(VARADA_SETUP, "DOUBLE size is not equal to Double size");
        }
        if (INTEGER.getFixedSize() != Integer.BYTES) {
            throw new TrinoException(VARADA_SETUP, "INTEGER size is not equal to Integer size");
        }
        if (TIMESTAMP_MILLIS.getFixedSize() != Long.BYTES) {
            throw new TrinoException(VARADA_SETUP, "TIMESTAMP size is not equal to Long size");
        }
        if (TIMESTAMP_TZ_MILLIS.getFixedSize() != Long.BYTES) {
            throw new TrinoException(VARADA_SETUP, "TIMESTAMP_WITH_TIME_ZONE size is not equal to Long size");
        }
        if (TIME_MILLIS.getFixedSize() != Long.BYTES) {
            throw new TrinoException(VARADA_SETUP, "TIME size is not equal to Long size");
        }
    }

    public DispatcherConnectorFactory getConnectorFactory()
    {
        return new DispatcherConnectorFactory(storageEngineModule, cloudVendorModule);
    }

    @VisibleForTesting
    public CachingPlugin withStorageEngineModule(Module module)
    {
        this.storageEngineModule = module;
        return this;
    }

    static {
        // assumptions verification
        verifyTypeSizes();
    }
}
