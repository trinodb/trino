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
package org.apache.iceberg;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.util.function.Function;

public final class TrinoInternalData
{
    private TrinoInternalData() {}

    /**
     * Replaces the reader and writer the library uses for metadata files, like partition statistics files, in {@code format}.
     * {@link InternalData#register} is package-private, and the library has no public way to replace them.
     */
    public static void register(FileFormat format, Function<OutputFile, InternalData.WriteBuilder> writeBuilder, Function<InputFile, InternalData.ReadBuilder> readBuilder)
    {
        InternalData.register(format, writeBuilder, readBuilder);
    }
}
