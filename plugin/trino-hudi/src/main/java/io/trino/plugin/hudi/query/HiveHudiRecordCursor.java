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

package io.trino.plugin.hudi.query;

import io.trino.plugin.hive.GenericHiveRecordCursor;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.RecordCursor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.util.List;
import java.util.Properties;

public final class HiveHudiRecordCursor
{
    private HiveHudiRecordCursor() {}

    public static RecordCursor createRecordCursor(
            Configuration configuration,
            Path path,
            RecordReader<?, ? extends Writable> recordReader,
            long totalBytes,
            Properties hiveSchema,
            List<HiveColumnHandle> hiveColumnHandles)
    {
        return new GenericHiveRecordCursor<>(
                configuration,
                path,
                recordReader,
                totalBytes,
                hiveSchema,
                hiveColumnHandles);
    }
}
