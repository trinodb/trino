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
package io.trino.plugin.hive.line;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hive.formats.line.sequence.SequenceFileWriterFactory;
import io.trino.hive.formats.line.simple.SimpleSerializerFactory;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

public class SimpleSequenceFileWriterFactory
        extends LineFileWriterFactory
{
    @Inject
    public SimpleSequenceFileWriterFactory(TrinoFileSystemFactory trinoFileSystemFactory, TypeManager typeManager, NodeVersion nodeVersion)
    {
        super(trinoFileSystemFactory,
                typeManager,
                new SimpleSerializerFactory(),
                new SequenceFileWriterFactory(nodeVersion.toString()),
                HiveSessionProperties::isSequenceFileNativeWriterEnabled,
                false);
    }
}
