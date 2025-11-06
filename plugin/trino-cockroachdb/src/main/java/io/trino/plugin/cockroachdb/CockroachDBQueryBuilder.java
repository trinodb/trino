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
package io.trino.plugin.cockroachdb;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcRelationHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.postgresql.CollationAwareQueryBuilder;

import java.util.function.Consumer;

public class CockroachDBQueryBuilder
        extends CollationAwareQueryBuilder
{
    @Inject
    public CockroachDBQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String getFrom(JdbcClient client, JdbcRelationHandle baseRelation, Consumer<QueryParameter> accumulator)
    {
        if (baseRelation instanceof JdbcNamedRelationHandle jdbcNamedRelationHandle && jdbcNamedRelationHandle.getReadVersion().isPresent()) {
            return super.getFrom(client, baseRelation, accumulator) + " AS OF SYSTEM TIME '" + jdbcNamedRelationHandle.getReadVersion().get() + "'";
        }
        else {
            return super.getFrom(client, baseRelation, accumulator);
        }
    }
}
