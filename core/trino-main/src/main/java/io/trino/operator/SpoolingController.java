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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.Page;

import static com.google.common.base.Verify.verify;

public interface SpoolingController
{
    enum Mode
    {
        INLINE,
        BUFFER,
        SPOOL
    }

    default Mode nextMode(Page page)
    {
        return nextMode(page.getPositionCount(), page.getSizeInBytes());
    }

    Mode nextMode(int positions, long size);

    Mode execute(Mode mode, long positions, long size);

    default void finish()
    {
        execute(Mode.SPOOL, 0, 0);
    }

    MetricSnapshot getMetrics(Mode mode);

    @VisibleForTesting
    default <T extends SpoolingController> T unwrap(Class<T> clazz)
    {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        throw new IllegalArgumentException("Cannot unwrap " + this + " to class " + clazz);
    }

    record MetricSnapshot(long positions, long size, long pages)
    {
        public MetricSnapshot
        {
            verify(positions >= 0, "Positions are expected to be non-negative");
            verify(size >= 0, "Size is expected to be non-negative");
            verify(pages >= 0, "Pages are expected to be non-negative");
        }
    }

    class ActionMetrics
    {
        private long pages;
        private long size;
        private long positions;

        public void recordPage(long positions, long size)
        {
            verify(positions >= 0, "Expected positions to be non-negative");
            verify(size >= 0, "Expected size to be non-negative");
            this.pages++;
            this.positions += positions;
            this.size += size;
        }

        public void reset()
        {
            this.pages = 0;
            this.size = 0;
            this.positions = 0;
        }

        public boolean isEmpty()
        {
            return pages == 0 && size == 0 && positions == 0;
        }

        public MetricSnapshot snapshot()
        {
            return new MetricSnapshot(positions, size, pages);
        }
    }
}
