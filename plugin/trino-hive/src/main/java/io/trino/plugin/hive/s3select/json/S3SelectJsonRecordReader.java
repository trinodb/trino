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
package io.trino.plugin.hive.s3select.json;

import io.trino.plugin.hive.s3select.S3SelectLineRecordReader;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.JSONType;
import software.amazon.awssdk.services.s3.model.OutputSerialization;

import java.util.Properties;

public class S3SelectJsonRecordReader
        extends S3SelectLineRecordReader
{
    public S3SelectJsonRecordReader(Configuration configuration,
                             Path path,
                             long start,
                             long length,
                             Properties schema,
                             String ionSqlQuery,
                             TrinoS3ClientFactory s3ClientFactory)
    {
        super(configuration, path, start, length, schema, ionSqlQuery, s3ClientFactory);
    }

    @Override
    public InputSerialization buildInputSerialization()
    {
        JSONInput.Builder selectObjectJSONInputSerialization = JSONInput.builder();
        selectObjectJSONInputSerialization.type(JSONType.LINES);

        InputSerialization.Builder selectObjectInputSerialization = InputSerialization.builder();
        selectObjectInputSerialization.compressionType(getCompressionType());
        selectObjectInputSerialization.json(selectObjectJSONInputSerialization.build());

        return selectObjectInputSerialization.build();
    }

    @Override
    public OutputSerialization buildOutputSerialization()
    {
        OutputSerialization.Builder selectObjectOutputSerialization = OutputSerialization.builder();
        JSONOutput selectObjectJSONOutputSerialization = JSONOutput.builder().build();
        selectObjectOutputSerialization.json(selectObjectJSONOutputSerialization);

        return selectObjectOutputSerialization.build();
    }

    @Override
    public boolean shouldEnableScanRange()
    {
        return CompressionType.NONE.equals(getCompressionType());
    }
}
