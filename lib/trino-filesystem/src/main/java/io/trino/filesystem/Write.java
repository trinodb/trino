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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Represents ongoing write operation.
 */
public interface Write
        extends AutoCloseable
{
    /**
     * The output stream to write to. The caller is not responsible for closing the stream.
     *
     * @see #close()
     */
    OutputStream stream();

    /**
     * Marks the write as finished. No more data can be written after this method is called.
     * It is an error to call this method after {@link #abort()} has been called.
     * The write operation is not guaranteed to be made visible when this method returns,
     * it will be made visible at the latest when {@link #close()} is called.
     */
    void finish();

    /**
     * Marks the write as aborted. No more data can be written after this method is called.
     * It is an error to call this method after {@link #finish()} has been called.
     * <p>
     * The effect of calling this method followed by {@link #close()} is the same as calling
     * {@link #close()} directly. Calling this method is useful for documenting the intent
     * and verify the {@link #finish()} has not been called.
     */
    void abort();

    /**
     * Releases any resources associated with this write operation, committing it if and only
     * if {@link #finish()} has not been called. It is an error to call this method if neither
     * {@link #finish()} nor {@link #abort()} has been called. In such case, it behaves as if
     * #abort() has been called, but throws an exception instead of returning.
     *
     * @throws IllegalStateException if the write has not been finished nor explicitly aborted
     *
     * @apiNote IllegalStateException is thrown when {@link #finish()}/{@link #close()} has not
     * been called to make it easier to use the Write API correctly.
     */
    @Override
    void close()
            throws IOException;
}
