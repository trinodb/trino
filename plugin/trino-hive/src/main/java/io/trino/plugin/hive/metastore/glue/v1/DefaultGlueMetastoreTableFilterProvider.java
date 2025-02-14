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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.services.glue.model.Table;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.hive.HideDeltaLakeTables;

import java.util.function.Predicate;

import static io.trino.plugin.hive.metastore.glue.v1.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static java.util.function.Predicate.not;

public class DefaultGlueMetastoreTableFilterProvider
        implements Provider<Predicate<Table>>
{
    private final boolean hideDeltaLakeTables;

    @Inject
    public DefaultGlueMetastoreTableFilterProvider(@HideDeltaLakeTables boolean hideDeltaLakeTables)
    {
        this.hideDeltaLakeTables = hideDeltaLakeTables;
    }

    @Override
    public Predicate<Table> get()
    {
        if (hideDeltaLakeTables) {
            return not(table -> isDeltaLakeTable(getTableParameters(table)));
        }
        return table -> true;
    }
}
