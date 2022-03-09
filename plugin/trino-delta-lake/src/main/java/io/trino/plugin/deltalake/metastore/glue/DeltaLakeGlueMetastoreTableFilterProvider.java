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
package io.trino.plugin.deltalake.metastore.glue;

import com.amazonaws.services.glue.model.Table;
import io.trino.plugin.hive.metastore.glue.DefaultGlueMetastoreTableFilterProvider;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Map;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class DeltaLakeGlueMetastoreTableFilterProvider
        implements Provider<Predicate<Table>>
{
    private final boolean hideNonDeltaLakeTables;

    @Inject
    public DeltaLakeGlueMetastoreTableFilterProvider(DeltaLakeGlueMetastoreConfig config)
    {
        requireNonNull(config, "config is null");
        this.hideNonDeltaLakeTables = config.isHideNonDeltaLakeTables();
    }

    @Override
    public Predicate<Table> get()
    {
        if (hideNonDeltaLakeTables) {
            return DeltaLakeGlueMetastoreTableFilterProvider::isDeltaLakeTable;
        }
        return table -> true;
    }

    private static boolean isDeltaLakeTable(Table table)
    {
        Map<String, String> parameters = table.getParameters();
        if (parameters == null) {
            return false;
        }
        // todo; add parameters == null check to DefaultGlueMetastoreTableFilterProvider.isDeltaLakeTable
        return DefaultGlueMetastoreTableFilterProvider.isDeltaLakeTable(table);
    }
}
