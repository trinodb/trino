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
package io.prestosql.plugin.hive.s3select;

import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import io.prestosql.plugin.hive.s3.HiveS3Config;
import io.prestosql.plugin.hive.s3.PrestoS3FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.Properties;

import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_CSV_COMMENTS;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.QUOTE_CHAR;

class S3SelectCsvRecordReader
        extends S3SelectLineRecordReader
{
    public S3SelectCsvRecordReader(
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties schema,
            String ionSqlQuery,
            PrestoS3ClientFactory s3ClientFactory)
    {
        super(configuration, path, start, length, schema, ionSqlQuery, s3ClientFactory);
    }

    @Override
    public SelectObjectContentRequest buildSelectObjectRequest(Properties schema, String query, Path path, Configuration configuration)
    {
        HiveS3Config defaults = new HiveS3Config();
        String s3CsvComments = configuration.get(S3_CSV_COMMENTS, defaults.getS3CsvComments());

        SelectObjectContentRequest selectObjectRequest = new SelectObjectContentRequest();
        URI uri = path.toUri();
        selectObjectRequest.setBucketName(PrestoS3FileSystem.extractBucketName(uri));
        selectObjectRequest.setKey(PrestoS3FileSystem.keyFromPath(path));
        selectObjectRequest.setExpression(query);
        selectObjectRequest.setExpressionType(ExpressionType.SQL);

        String fieldDelimiter = getFieldDelimiter(schema);
        String quoteChar = schema.getProperty(QUOTE_CHAR, null);
        String escapeChar = schema.getProperty(ESCAPE_CHAR, null);

        CSVInput selectObjectCSVInputSerialization = new CSVInput();
        selectObjectCSVInputSerialization.setRecordDelimiter(lineDelimiter);
        selectObjectCSVInputSerialization.setFieldDelimiter(fieldDelimiter);
        selectObjectCSVInputSerialization.setComments(s3CsvComments);
        selectObjectCSVInputSerialization.setQuoteCharacter(quoteChar);
        selectObjectCSVInputSerialization.setQuoteEscapeCharacter(escapeChar);

        InputSerialization selectObjectInputSerialization = new InputSerialization();
        selectObjectInputSerialization.setCompressionType(getCompressionType(path));
        selectObjectInputSerialization.setCsv(selectObjectCSVInputSerialization);
        selectObjectRequest.setInputSerialization(selectObjectInputSerialization);

        OutputSerialization selectObjectOutputSerialization = new OutputSerialization();
        CSVOutput selectObjectCSVOutputSerialization = new CSVOutput();
        selectObjectCSVOutputSerialization.setRecordDelimiter(lineDelimiter);
        selectObjectCSVOutputSerialization.setFieldDelimiter(fieldDelimiter);
        selectObjectCSVOutputSerialization.setQuoteCharacter(quoteChar);
        selectObjectCSVOutputSerialization.setQuoteEscapeCharacter(escapeChar);
        selectObjectOutputSerialization.setCsv(selectObjectCSVOutputSerialization);
        selectObjectRequest.setOutputSerialization(selectObjectOutputSerialization);

        return selectObjectRequest;
    }
}
