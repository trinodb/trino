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
package io.trino.filesystem;

import java.util.concurrent.atomic.AtomicLong;

public class InputFileMetrics
{
    public static final InputFileMetrics EMPTY = new InputFileMetrics() {
        @Override
        public void recordCacheRead(int bytes)
        {
            throw new UnsupportedOperationException("Cannot record cache read on empty metrics");
        }

        @Override
        public void recordExternalRead(int bytes)
        {
            throw new UnsupportedOperationException("Cannot record external read on empty metrics");
        }

        @Override
        public long getBytesReadExternally()
        {
            return 0;
        }

        @Override
        public long getBytesReadFromCache()
        {
            return 0;
        }
    };

    private final AtomicLong bytesReadExternally = new AtomicLong();
    private final AtomicLong bytesReadFromCache = new AtomicLong();

    public long getBytesReadExternally()
    {
        return bytesReadExternally.get();
    }

    public long getBytesReadFromCache()
    {
        return bytesReadFromCache.get();
    }

    public void recordCacheRead(int bytes)
    {
        bytesReadFromCache.addAndGet(bytes);
    }

    public void recordExternalRead(int bytes)
    {
        bytesReadExternally.addAndGet(bytes);
    }
}
