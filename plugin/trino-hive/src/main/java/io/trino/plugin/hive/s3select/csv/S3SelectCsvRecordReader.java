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
package io.trino.plugin.hive.s3select.csv;

import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.ScanRange;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import io.trino.plugin.hive.s3.TrinoS3FileSystem;
import io.trino.plugin.hive.s3select.S3SelectLineRecordReader;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.Properties;

import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.QUOTE_CHAR;

public class S3SelectCsvRecordReader
        extends S3SelectLineRecordReader
{
    /*
     * Sentinel unicode comment character (http://www.unicode.org/faq/private_use.html#nonchar_codes).
     * It is expected that \uFDD0 sentinel comment character is not the first character in any row of user's CSV S3 object.
     * The rows starting with \uFDD0 will be skipped by S3Select and will not be a part of the result set or aggregations.
     * To process CSV objects that may contain \uFDD0 as first row character please disable S3SelectPushdown.
     * TODO: Remove this proxy logic when S3Select API supports disabling of row level comments.
     */

    private static final String COMMENTS_CHAR_STR = "\uFDD0";
    private static final String DEFAULT_FIELD_DELIMITER = ",";

    public S3SelectCsvRecordReader(
            Configuration configuration,
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
    public SelectObjectContentRequest buildSelectObjectRequest(Properties schema, String query, Path path)
    {
        SelectObjectContentRequest selectObjectRequest = new SelectObjectContentRequest();
        URI uri = path.toUri();
        selectObjectRequest.setBucketName(TrinoS3FileSystem.extractBucketName(uri));
        selectObjectRequest.setKey(TrinoS3FileSystem.keyFromPath(path));
        selectObjectRequest.setExpression(query);
        selectObjectRequest.setExpressionType(ExpressionType.SQL);

        String fieldDelimiter = getFieldDelimiter(schema);
        String quoteChar = schema.getProperty(QUOTE_CHAR, null);
        String escapeChar = schema.getProperty(ESCAPE_CHAR, null);

        CSVInput selectObjectCSVInputSerialization = new CSVInput();
        selectObjectCSVInputSerialization.setRecordDelimiter(getLineDelimiter());
        selectObjectCSVInputSerialization.setFieldDelimiter(fieldDelimiter);
        selectObjectCSVInputSerialization.setComments(COMMENTS_CHAR_STR);
        selectObjectCSVInputSerialization.setQuoteCharacter(quoteChar);
        selectObjectCSVInputSerialization.setQuoteEscapeCharacter(escapeChar);

        InputSerialization selectObjectInputSerialization = new InputSerialization();
        CompressionType compressionType = getCompressionType(path);
        selectObjectInputSerialization.setCompressionType(compressionType);
        selectObjectInputSerialization.setCsv(selectObjectCSVInputSerialization);
        selectObjectRequest.setInputSerialization(selectObjectInputSerialization);

        OutputSerialization selectObjectOutputSerialization = new OutputSerialization();
        CSVOutput selectObjectCSVOutputSerialization = new CSVOutput();
        selectObjectCSVOutputSerialization.setRecordDelimiter(getLineDelimiter());
        selectObjectCSVOutputSerialization.setFieldDelimiter(fieldDelimiter);
        selectObjectCSVOutputSerialization.setQuoteCharacter(quoteChar);
        selectObjectCSVOutputSerialization.setQuoteEscapeCharacter(escapeChar);
        selectObjectOutputSerialization.setCsv(selectObjectCSVOutputSerialization);
        selectObjectRequest.setOutputSerialization(selectObjectOutputSerialization);

        // Scan range requests to S3 Select allow us to enable splits,
        // by providing a finer granularity than file level.
        // Works for CSV if AllowQuotedRecordDelimiter is disabled.
        boolean isQuotedRecordDelimiterAllowed = Boolean.TRUE.equals(
                selectObjectCSVInputSerialization.getAllowQuotedRecordDelimiter());
        if (CompressionType.NONE.equals(compressionType) && !isQuotedRecordDelimiterAllowed) {
            // Scan range queries are not supported for compressed files.
            ScanRange scanRange = new ScanRange();
            scanRange.setStart(getStart());
            scanRange.setEnd(getEnd());
            selectObjectRequest.setScanRange(scanRange);
        }

        return selectObjectRequest;
    }

    protected String getFieldDelimiter(Properties schema)
    {
        // Use the field delimiter only if it is specified in the schema. If not, use a default field delimiter ','
        return schema.getProperty(FIELD_DELIM, DEFAULT_FIELD_DELIMITER);
    }
}
