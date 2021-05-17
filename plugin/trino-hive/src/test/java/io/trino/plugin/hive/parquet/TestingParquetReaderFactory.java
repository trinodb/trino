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
package io.trino.plugin.hive.parquet;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Types;
import io.airlift.bootstrap.Bootstrap;
import io.trino.parquet.reader.ColumnReaderFactory;
import io.trino.parquet.reader.ColumnReaderFactory.ColumnReaderModule;
import io.trino.parquet.reader.ColumnReaderFactory.ColumnReaderProvider;
import io.trino.parquet.reader.ParquetReader.ParquetReaderFactory;

import java.util.Set;

public class TestingParquetReaderFactory
        extends ParquetReaderFactory
{
    public TestingParquetReaderFactory()
    {
        super(getColumnReaderFactory());
    }

    private static ColumnReaderFactory getColumnReaderFactory()
    {
        Injector injector = new Bootstrap(new ColumnReaderModule()).initialize();
        Set<ColumnReaderProvider> providers =
                injector.getInstance(Key.get((TypeLiteral<Set<ColumnReaderProvider>>) TypeLiteral.get(Types.setOf(ColumnReaderProvider.class))));

        return new ColumnReaderFactory(providers);
    }
}
