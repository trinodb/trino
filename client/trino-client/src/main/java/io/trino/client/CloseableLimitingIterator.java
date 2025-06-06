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
package io.trino.client;

import com.google.common.collect.AbstractIterator;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class CloseableLimitingIterator<T>
        extends AbstractIterator<T>
        implements CloseableIterator<T>
{
    private final long maxRows;
    private final CloseableIterator<T> delegate;
    private long currentRow;

    CloseableLimitingIterator(CloseableIterator<T> delegate, long maxRows)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.maxRows = maxRows;
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    protected T computeNext()
    {
        if (maxRows > 0 && currentRow >= maxRows) {
            try {
                close();
            }
            catch (IOException ignored) {
            }
            return endOfData();
        }
        currentRow++;
        if (delegate.hasNext()) {
            return delegate.next();
        }
        return endOfData();
    }

    @Override
    public String toString()
    {
        return "CloseableLimitingIterator{delegate=" + delegate + ", maxRows=" + maxRows + "}";
    }

    public static <T> CloseableIterator<T> limit(CloseableIterator<T> delegate, long maxRows)
    {
        return new CloseableLimitingIterator<>(delegate, maxRows);
    }
}
