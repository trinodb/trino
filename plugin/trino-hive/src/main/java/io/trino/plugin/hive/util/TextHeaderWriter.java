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
package io.trino.plugin.hive.util;

import io.trino.plugin.hive.HiveType;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static java.util.Collections.nCopies;

public class TextHeaderWriter
{
    private final Serializer serializer;
    private final Type headerType;
    private final List<String> fileColumnNames;

    public TextHeaderWriter(Serializer serializer, TypeManager typeManager, ConnectorSession session, List<String> fileColumnNames)
    {
        this.serializer = serializer;
        this.fileColumnNames = fileColumnNames;
        this.headerType = HiveType.valueOf("string").getType(typeManager, getTimestampPrecision(session));
    }

    public void write(OutputStream compressedOutput, int rowSeparator)
            throws IOException
    {
        try {
            ObjectInspector stringObjectInspector = HiveWriteUtils.getRowColumnInspector(headerType);
            List<Text> headers = fileColumnNames.stream().map(Text::new).collect(toImmutableList());
            List<ObjectInspector> inspectors = nCopies(fileColumnNames.size(), stringObjectInspector);
            StandardStructObjectInspector headerStructObjectInspectors = ObjectInspectorFactory.getStandardStructObjectInspector(fileColumnNames, inspectors);
            BinaryComparable binary = (BinaryComparable) serializer.serialize(headers, headerStructObjectInspectors);
            compressedOutput.write(binary.getBytes(), 0, binary.getLength());
            compressedOutput.write(rowSeparator);
        }
        catch (SerDeException e) {
            throw new IOException(e);
        }
    }
}
