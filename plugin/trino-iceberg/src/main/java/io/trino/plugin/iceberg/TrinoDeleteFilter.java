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
package io.trino.plugin.iceberg;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

public class TrinoDeleteFilter
        extends DeleteFilter<TrinoRow>
{
    private final FileIO fileIO;

    public TrinoDeleteFilter(FileIO fileIO, FileScanTask task, Schema tableSchema, Schema requestedSchema)
    {
        super(task, tableSchema, requestedSchema);
        this.fileIO = fileIO;
    }

    @Override
    protected StructLike asStructLike(TrinoRow trinoRow)
    {
        return trinoRow;
    }

    @Override
    protected InputFile getInputFile(String s)
    {
        return fileIO.newInputFile(s);
    }
}
