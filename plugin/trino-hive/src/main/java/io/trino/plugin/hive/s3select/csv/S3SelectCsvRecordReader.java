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
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import io.trino.plugin.hive.s3select.S3SelectLineRecordReader;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

import static io.trino.plugin.hive.util.SerdeConstants.ESCAPE_CHAR;
import static io.trino.plugin.hive.util.SerdeConstants.FIELD_DELIM;
import static io.trino.plugin.hive.util.SerdeConstants.QUOTE_CHAR;

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
    public InputSerialization buildInputSerialization()
    {
        Properties schema = getSchema();
        String fieldDelimiter = schema.getProperty(FIELD_DELIM, DEFAULT_FIELD_DELIMITER);
        String quoteChar = schema.getProperty(QUOTE_CHAR, null);
        String escapeChar = schema.getProperty(ESCAPE_CHAR, null);

        CSVInput selectObjectCSVInputSerialization = new CSVInput();
        selectObjectCSVInputSerialization.setRecordDelimiter(getLineDelimiter());
        selectObjectCSVInputSerialization.setFieldDelimiter(fieldDelimiter);
        selectObjectCSVInputSerialization.setComments(COMMENTS_CHAR_STR);
        selectObjectCSVInputSerialization.setQuoteCharacter(quoteChar);
        selectObjectCSVInputSerialization.setQuoteEscapeCharacter(escapeChar);

        InputSerialization selectObjectInputSerialization = new InputSerialization();
        selectObjectInputSerialization.setCompressionType(getCompressionType());
        selectObjectInputSerialization.setCsv(selectObjectCSVInputSerialization);

        return selectObjectInputSerialization;
    }

    @Override
    public OutputSerialization buildOutputSerialization()
    {
        Properties schema = getSchema();
        String fieldDelimiter = schema.getProperty(FIELD_DELIM, DEFAULT_FIELD_DELIMITER);
        String quoteChar = schema.getProperty(QUOTE_CHAR, null);
        String escapeChar = schema.getProperty(ESCAPE_CHAR, null);

        OutputSerialization selectObjectOutputSerialization = new OutputSerialization();
        CSVOutput selectObjectCSVOutputSerialization = new CSVOutput();
        selectObjectCSVOutputSerialization.setRecordDelimiter(getLineDelimiter());
        selectObjectCSVOutputSerialization.setFieldDelimiter(fieldDelimiter);
        selectObjectCSVOutputSerialization.setQuoteCharacter(quoteChar);
        selectObjectCSVOutputSerialization.setQuoteEscapeCharacter(escapeChar);
        selectObjectOutputSerialization.setCsv(selectObjectCSVOutputSerialization);

        return selectObjectOutputSerialization;
    }

    @Override
    public boolean shouldEnableScanRange()
    {
        // Works for CSV if AllowQuotedRecordDelimiter is disabled.
        boolean isQuotedRecordDelimiterAllowed = Boolean.TRUE.equals(
                buildInputSerialization().getCsv().getAllowQuotedRecordDelimiter());
        return CompressionType.NONE.equals(getCompressionType()) && !isQuotedRecordDelimiterAllowed;
    }
}
