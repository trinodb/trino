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
package io.trino.hive.formats.line.csv;

import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineDeserializerFactory;

import java.util.List;
import java.util.Map;

import static io.trino.hive.formats.line.csv.CsvConstants.DEFAULT_QUOTE;
import static io.trino.hive.formats.line.csv.CsvConstants.DEFAULT_SEPARATOR;
import static io.trino.hive.formats.line.csv.CsvConstants.DESERIALIZER_DEFAULT_ESCAPE;
import static io.trino.hive.formats.line.csv.CsvConstants.ESCAPE_KEY;
import static io.trino.hive.formats.line.csv.CsvConstants.HIVE_SERDE_CLASS_NAMES;
import static io.trino.hive.formats.line.csv.CsvConstants.QUOTE_KEY;
import static io.trino.hive.formats.line.csv.CsvConstants.SEPARATOR_KEY;
import static io.trino.hive.formats.line.csv.CsvConstants.getCharProperty;

public class CsvDeserializerFactory
        implements LineDeserializerFactory
{
    @Override
    public List<String> getHiveSerDeClassNames()
    {
        return HIVE_SERDE_CLASS_NAMES;
    }

    @Override
    public LineDeserializer create(List<Column> columns, Map<String, String> serdeProperties)
    {
        char separatorChar = getCharProperty(serdeProperties, SEPARATOR_KEY, DEFAULT_SEPARATOR);
        char quoteChar = getCharProperty(serdeProperties, QUOTE_KEY, DEFAULT_QUOTE);
        char escapeChar = getCharProperty(serdeProperties, ESCAPE_KEY, DESERIALIZER_DEFAULT_ESCAPE);
        return new CsvDeserializer(columns, separatorChar, quoteChar, escapeChar);
    }
}
