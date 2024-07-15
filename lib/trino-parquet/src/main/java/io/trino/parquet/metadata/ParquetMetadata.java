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
package io.trino.parquet.metadata;

import java.util.List;

public class ParquetMetadata
{
    private final FileMetadata fileMetaData;
    private final List<BlockMetadata> blocks;

    public ParquetMetadata(FileMetadata fileMetaData, List<BlockMetadata> blocks)
    {
        this.fileMetaData = fileMetaData;
        this.blocks = blocks;
    }

    public List<BlockMetadata> getBlocks()
    {
        return blocks;
    }

    public FileMetadata getFileMetaData()
    {
        return fileMetaData;
    }

    @Override
    public String toString()
    {
        return "ParquetMetaData{" + fileMetaData + ", blocks: " + blocks + "}";
    }
}
