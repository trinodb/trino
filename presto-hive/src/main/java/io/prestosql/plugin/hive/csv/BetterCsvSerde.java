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

package io.prestosql.plugin.hive.csv;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * BetterCsvSerde use univocity to deserialize CSV format.
 * Users can specify custom separator, quote or escape characters. And the default separator(,),
 * quote("), and escape characters(") are the same as the univocity library defaults.
 *
 */
@SerDeSpec(schemaProps = {
        serdeConstants.LIST_COLUMNS,
        BetterCsvSerde.SEPARATORCHAR, BetterCsvSerde.QUOTECHAR, BetterCsvSerde.ESCAPECHAR})
public class BetterCsvSerde
        extends AbstractSerDe
{
    private ObjectInspector inspector;
    private String[] outputFields;
    private int numCols;
    private List<String> row;

    private char separatorChar;
    private char quoteChar;
    private char escapeChar;

    private CsvParser csvParser;
    private CsvWriterSettings csvWriterSettings;

    public static final String SEPARATORCHAR = "separatorChar";
    public static final String QUOTECHAR = "quoteChar";
    public static final String ESCAPECHAR = "escapeChar";

    @Override
    public void initialize(final Configuration conf, final Properties tbl) throws SerDeException
    {
        final List<String> columnNames = Arrays.asList(tbl.getProperty(serdeConstants.LIST_COLUMNS)
                .split(","));

        numCols = columnNames.size();

        final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols);

        for (int i = 0; i < numCols; i++) {
            columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
        outputFields = new String[numCols];
        row = new ArrayList<String>(numCols);

        for (int i = 0; i < numCols; i++) {
            row.add(null);
        }

        separatorChar = getProperty(tbl, SEPARATORCHAR, ',');
        quoteChar = getProperty(tbl, QUOTECHAR, '"');
        escapeChar = getProperty(tbl, ESCAPECHAR, '"');

        CsvFormat format = new CsvFormat();
        format.setQuote(quoteChar);
        format.setQuoteEscape(escapeChar);
        format.setDelimiter(separatorChar);
        CsvParserSettings settings = new CsvParserSettings();
        settings.setFormat(format);
        settings.setNullValue("");
        csvParser = new CsvParser(settings);

        csvWriterSettings = new CsvWriterSettings();
        csvWriterSettings.setFormat(format);
    }

    private char getProperty(final Properties tbl, final String property, final char def)
    {
        final String val = tbl.getProperty(property);

        if (val != null) {
            return val.charAt(0);
        }

        return def;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException
    {
        final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
        final List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();

        if (outputFieldRefs.size() != numCols) {
            throw new SerDeException("Cannot serialize the object because there are "
                            + outputFieldRefs.size() + " fields but the table has " + numCols + " columns.");
        }

        // Get all data out.
        for (int c = 0; c < numCols; c++) {
            final Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
            final ObjectInspector fieldOI = outputFieldRefs.get(c).getFieldObjectInspector();

            // The data must be of type String
            final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;

            // Convert the field to Java class String, because objects of String type
            // can be stored in String, Text, or some other classes.
            outputFields[c] = fieldStringOI.getPrimitiveJavaObject(field);
        }

        final StringWriter writer = new StringWriter();
        final CsvWriter csv = new CsvWriter(writer, csvWriterSettings);
        csv.writeRow(outputFields);
        csv.close();
        return new Text(writer.toString().stripTrailing());
    }

    @Override
    public Object deserialize(final Writable blob) throws SerDeException
    {
        Text rowText = (Text) blob;
        final String[] read = csvParser.parseLine(rowText.toString());

        for (int i = 0; i < numCols; i++) {
            if (read != null && i < read.length) {
                row.set(i, read[i]);
            }
            else {
                row.set(i, null);
            }
        }
        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException
    {
        return inspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass()
    {
        return Text.class;
    }

    @Override
    public SerDeStats getSerDeStats()
    {
        return null;
    }
}
