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
package io.trino.plugin.hive;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.avro.AvroFileWriterFactory;
import io.trino.plugin.hive.avro.AvroPageSourceFactory;
import io.trino.plugin.hive.crypto.ParquetEncryptionModule;
import io.trino.plugin.hive.esri.EsriPageSourceFactory;
import io.trino.plugin.hive.line.CsvFileWriterFactory;
import io.trino.plugin.hive.line.CsvPageSourceFactory;
import io.trino.plugin.hive.line.JsonFileWriterFactory;
import io.trino.plugin.hive.line.JsonPageSourceFactory;
import io.trino.plugin.hive.line.OpenXJsonFileWriterFactory;
import io.trino.plugin.hive.line.OpenXJsonPageSourceFactory;
import io.trino.plugin.hive.line.ProtobufSequenceFilePageSourceFactory;
import io.trino.plugin.hive.line.RegexFileWriterFactory;
import io.trino.plugin.hive.line.RegexPageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFileWriterFactory;
import io.trino.plugin.hive.line.SimpleTextFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleTextFileWriterFactory;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetFileWriterFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveFormatsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(CsvPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(JsonPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(OpenXJsonPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(EsriPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RegexPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(SimpleTextFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(SimpleSequenceFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(AvroPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ProtobufSequenceFilePageSourceFactory.class).in(Scopes.SINGLETON);

        binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrcFileWriterFactory.class).withGeneratedName();

        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        fileWriterFactoryBinder.addBinding().to(CsvFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(JsonFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RegexFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(OpenXJsonFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(SimpleTextFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(SimpleSequenceFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(AvroFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(ParquetFileWriterFactory.class).in(Scopes.SINGLETON);

        install(new ParquetEncryptionModule());
    }
}
