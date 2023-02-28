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
package io.trino.hive.formats.line.simple;

import com.google.common.base.Splitter;
import io.trino.hive.formats.encodings.text.TextEncodingOptions;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineDeserializerFactory;

import java.util.List;
import java.util.Map;

import static io.trino.hive.formats.line.simple.SimpleConstants.HIVE_SERDE_CLASS_NAMES;

public class SimpleDeserializerFactory
        implements LineDeserializerFactory
{
    private static final Splitter COLUMN_NAMES_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final String LIST_COLUMNS = "columns";

    @Override
    public List<String> getHiveSerDeClassNames()
    {
        return HIVE_SERDE_CLASS_NAMES;
    }

    @Override
    public LineDeserializer create(List<Column> columns, Map<String, String> serdeProperties)
    {
        int tableColumnCount = COLUMN_NAMES_SPLITTER.splitToList(serdeProperties.getOrDefault(LIST_COLUMNS, "")).size();
        TextEncodingOptions textEncodingOptions = TextEncodingOptions.fromSchema(serdeProperties);
        return new SimpleDeserializer(columns, textEncodingOptions, tableColumnCount);
    }
}
