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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.NamespaceName;
import io.trino.metastore.Database;

import java.util.Optional;

import static io.trino.metastore.lakehouse.StorageMapper.optionalText;

/**
 * Maps a Lakehouse database to the Trino metastore model.
 */
public final class DatabaseMapper
{
    private DatabaseMapper() {}

    public static Database toTrino(HiveDatabase database)
    {
        return new Database(
                databaseName(database),
                optionalText(database.getLocationUri()),
                // The Lakehouse Hive database message has no owner, so the owner name and type stay empty
                Optional.empty(),
                Optional.empty(),
                optionalText(database.getDescription()),
                database.getParametersMap());
    }

    /**
     * Returns the database name, which Lakehouse reports as part of a resource name.
     */
    public static String databaseName(HiveDatabase database)
    {
        return NamespaceName.parse(database.getName()).getDatabase();
    }
}
