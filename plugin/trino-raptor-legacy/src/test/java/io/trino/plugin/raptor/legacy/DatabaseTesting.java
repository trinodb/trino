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
package io.trino.plugin.raptor.legacy;

import io.trino.plugin.raptor.legacy.metadata.Distribution;
import io.trino.plugin.raptor.legacy.metadata.TableColumn;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.type.InternalTypeManager;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.concurrent.ThreadLocalRandom;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;

public final class DatabaseTesting
{
    private DatabaseTesting() {}

    public static Jdbi createTestingJdbi()
    {
        TypeManager typeManager = new InternalTypeManager(createTestMetadataManager(), new TypeOperators());
        return Jdbi.create("jdbc:h2:mem:test" + System.nanoTime() + ThreadLocalRandom.current().nextLong())
                .installPlugin(new SqlObjectPlugin())
                .registerRowMapper(new TableColumn.Mapper(typeManager))
                .registerRowMapper(new Distribution.Mapper(typeManager));
    }
}
