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
package io.trino;

import io.airlift.units.DataSize;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static java.lang.String.format;

public class ExceededMemoryLimitException
        extends TrinoException
{
    public static ExceededMemoryLimitException exceededGlobalUserLimit(DataSize maxMemory)
    {
        return new ExceededMemoryLimitException(EXCEEDED_GLOBAL_MEMORY_LIMIT, format("Query exceeded distributed user memory limit of %s", maxMemory));
    }

    public static ExceededMemoryLimitException exceededGlobalTotalLimit(DataSize maxMemory)
    {
        return new ExceededMemoryLimitException(EXCEEDED_GLOBAL_MEMORY_LIMIT, format("Query exceeded distributed total memory limit of %s", maxMemory));
    }

    public static ExceededMemoryLimitException exceededLocalUserMemoryLimit(DataSize maxMemory, String additionalFailureInfo)
    {
        return new ExceededMemoryLimitException(EXCEEDED_LOCAL_MEMORY_LIMIT,
                format("Query exceeded per-node memory limit of %s [%s]", maxMemory, additionalFailureInfo));
    }

    public static ExceededMemoryLimitException exceededTaskMemoryLimit(DataSize maxMemory, String additionalFailureInfo)
    {
        return new ExceededMemoryLimitException(EXCEEDED_LOCAL_MEMORY_LIMIT,
                format("Query exceeded per-task memory limit of %s [%s]", maxMemory, additionalFailureInfo));
    }

    private ExceededMemoryLimitException(StandardErrorCode errorCode, String message)
    {
        super(errorCode, message);
    }
}
