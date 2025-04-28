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
package io.trino.plugin.hudi.io;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieAvroBootstrapFileReader;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieNativeAvroHFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;

public class HudiTrinoFileReaderFactory
        extends HoodieFileReaderFactory
{
    public HudiTrinoFileReaderFactory(HoodieStorage storage)
    {
        super(storage);
    }

    @Override
    protected HoodieFileReader newParquetFileReader(StoragePath path)
    {
        throw new UnsupportedOperationException("HudiTrinoFileReaderFactory does not support Parquet file reader");
    }

    @Override
    protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig,
            StoragePath path,
            Option<Schema> schemaOption)
            throws IOException
    {
        return new HoodieNativeAvroHFileReader(storage, path, schemaOption);
    }

    @Override
    protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig,
            StoragePath path,
            HoodieStorage storage,
            byte[] content,
            Option<Schema> schemaOption)
            throws IOException
    {
        return new HoodieNativeAvroHFileReader(this.storage, content, schemaOption);
    }

    @Override
    protected HoodieFileReader newOrcFileReader(StoragePath path)
    {
        throw new UnsupportedOperationException("HudiTrinoFileReaderFactory does not support ORC file reader");
    }

    @Override
    public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader, HoodieFileReader dataFileReader, Option<String[]> partitionFields, Object[] partitionValues)
    {
        return new HoodieAvroBootstrapFileReader(skeletonFileReader, dataFileReader, partitionFields, partitionValues);
    }
}
