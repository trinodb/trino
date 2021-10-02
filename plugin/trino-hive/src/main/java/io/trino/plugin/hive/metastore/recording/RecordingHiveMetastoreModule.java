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
package io.trino.plugin.hive.metastore.recording;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.ForRecordingHiveMetastore;
import io.trino.plugin.hive.RecordingMetastoreConfig;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.ForCachingHiveMetastore;
import io.trino.plugin.hive.util.BlockJsonSerde;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class RecordingHiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        if (buildConfigObject(RecordingMetastoreConfig.class).getRecordingPath() != null) {
            install(new RecordingModule());
        }
        else {
            install(new NoRecordingModule());
        }
    }

    public static class RecordingModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(HiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(RecordingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
            binder.bind(RecordingHiveMetastore.class).in(Scopes.SINGLETON);
            binder.bind(HiveBlockEncodingSerde.class).in(Scopes.SINGLETON);

            binder.bind(HiveMetastoreRecording.class).in(Scopes.SINGLETON);
            jsonCodecBinder(binder).bindJsonCodec(HiveMetastoreRecording.Recording.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

            // export under the old name, for backwards compatibility
            newExporter(binder).export(HiveMetastoreRecording.class).as(generator -> generator.generatedNameOf(RecordingHiveMetastore.class));

            newSetBinder(binder, Procedure.class).addBinding().toProvider(WriteHiveMetastoreRecordingProcedure.class).in(Scopes.SINGLETON);
        }
    }

    public static class NoRecordingModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(HiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(Key.get(HiveMetastore.class, ForRecordingHiveMetastore.class))
                    .in(Scopes.SINGLETON);
        }
    }
}
