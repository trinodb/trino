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
package io.trino.hive.formats.line.text;

import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.LineWriter;
import io.trino.hive.formats.line.LineWriterFactory;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

public class TextLineWriterFactory
        implements LineWriterFactory
{
    @Override
    public String getHiveOutputFormatClassName()
    {
        return "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    }

    @Override
    public LineWriter createLineWriter(ConnectorSession session, OutputStream outputStream, Optional<CompressionKind> compressionKind)
            throws IOException
    {
        return new TextLineWriter(outputStream, compressionKind);
    }
}
