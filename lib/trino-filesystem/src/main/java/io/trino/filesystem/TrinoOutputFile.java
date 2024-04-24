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

import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;

public interface TrinoOutputFile
{
    /**
     * This method delegates to {@link #create(AggregatedMemoryContext)}.
     */
    default OutputStream create()
            throws IOException
    {
        return create(newSimpleAggregatedMemoryContext());
    }

    /**
     * Create file with the specified content, atomically if possible.
     * The file will be replaced if it already exists.
     * If an error occurs while writing and the implementation does not
     * support atomic writes, then a partial file may be written,
     * or the original file may be deleted or left unchanged.
     */
    void createOrOverwrite(byte[] data)
            throws IOException;

    /**
     * Create file exclusively and atomically with the specified content.
     * If an error occurs while writing, the file will not be created.
     *
     * @throws FileAlreadyExistsException if the file already exists
     * @throws UnsupportedOperationException if the file system does not support this operation
     */
    default void createExclusive(byte[] data)
            throws IOException
    {
        throw new UnsupportedOperationException("createExclusive not supported by " + getClass());
    }

    /**
     * Open an output stream for creating a new file.
     * This method is expected to be used with unique locations.
     * <p>
     * If the file already exists, the method may fail or the file may be overwritten,
     * depending on the file system implementation.
     * For example, S3 does not support creating files exclusively, and performing an
     * existence check before creating the file is both expensive and unnecessary for
     * locations that are expected to be unique.
     * <p>
     * The file may be created immediately, or it may be created atomically when the
     * output stream is closed, depending on the file system implementation.
     *
     * @throws FileAlreadyExistsException if the file already exists (optional)
     */
    OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException;

    Location location();
}
