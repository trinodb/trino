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
package io.trino.verifier;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.List;

public interface VerifierDao
{
    @SqlQuery(
            """
            SELECT
              suite
            , name
            , test_catalog
            , test_schema
            , test_prequeries
            , test_query
            , test_postqueries
            , test_username
            , test_password
            , test_session_properties_json
            , control_catalog
            , control_schema
            , control_prequeries
            , control_query
            , control_postqueries
            , control_username
            , control_password
            , control_session_properties_json
            FROM verifier_queries
            WHERE suite = :suite
            ORDER BY id
            LIMIT :limit
            """)
    @UseRowMapper(QueryPairMapper.class)
    List<QueryPair> getQueriesBySuite(@Bind("suite") String suite, @Bind("limit") int limit);
}
