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
package io.prestosql.plugin.iceberg;

import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.RecordFileWriter;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.Metrics;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class IcebergRecordFileWriter
        extends RecordFileWriter
        implements IcebergFileWriter
{
    public IcebergRecordFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            DataSize estimatedWriterSystemMemoryUsage,
            JobConf conf,
            TypeManager typeManager,
            ConnectorSession session)
    {
        super(path, inputColumnNames, storageFormat, schema, estimatedWriterSystemMemoryUsage, conf, typeManager, session);
    }

    @Override
    public Optional<Metrics> getMetrics()
    {
        return Optional.empty();
    }
}
