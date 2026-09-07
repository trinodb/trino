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
package io.trino.plugin.session.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.session.SessionMatchSpec;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.plugin.session.db.util.SessionPropertiesDaoUtil.CLIENT_TAGS_TABLE;
import static io.trino.plugin.session.db.util.SessionPropertiesDaoUtil.PROPERTIES_TABLE;
import static io.trino.plugin.session.db.util.SessionPropertiesDaoUtil.SESSION_SPECS_TABLE;

/**
 * Dao should guarantee that the list of SessionMatchSpecs is returned in increasing order of priority. i.e. if two
 * rows in the ResultSet specify different values for the same property, the row coming in later will override the
 * value set by the row coming in earlier.
 */
public interface SessionPropertiesDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS " + SESSION_SPECS_TABLE + "(\n" +
            "spec_id BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "user_regex VARCHAR(512),\n" +
            "source_regex VARCHAR(512),\n" +
            "query_type VARCHAR(512),\n" +
            "group_regex VARCHAR(512),\n" +
            "priority INT NOT NULL,\n" +
            "PRIMARY KEY (spec_id)\n" +
            ")")
    void createSessionSpecsTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS " + CLIENT_TAGS_TABLE + "(\n" +
            "tag_spec_id BIGINT NOT NULL,\n" +
            "client_tag VARCHAR(512) NOT NULL,\n" +
            "PRIMARY KEY (tag_spec_id, client_tag),\n" +
            "FOREIGN KEY (tag_spec_id) REFERENCES session_specs (spec_id)\n" +
            ")")
    void createSessionClientTagsTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS " + PROPERTIES_TABLE + "(\n" +
            "property_spec_id BIGINT NOT NULL,\n" +
            "session_property_name VARCHAR(512),\n" +
            "session_property_value VARCHAR(512),\n" +
            "PRIMARY KEY (property_spec_id, session_property_name),\n" +
            "FOREIGN KEY (property_spec_id) REFERENCES session_specs (spec_id)\n" +
            ")")
    void createSessionPropertiesTable();

    @SqlUpdate("DROP TABLE IF EXISTS " + SESSION_SPECS_TABLE)
    void dropSessionSpecsTable();

    @SqlUpdate("DROP TABLE IF EXISTS " + CLIENT_TAGS_TABLE)
    void dropSessionClientTagsTable();

    @SqlUpdate("DROP TABLE IF EXISTS " + PROPERTIES_TABLE)
    void dropSessionPropertiesTable();

    @SqlQuery("SELECT spec_id, user_regex, source_regex, query_type, group_regex, priority\n" +
            "FROM " + SESSION_SPECS_TABLE + "\n" +
            "ORDER BY priority ASC")
    @UseRowMapper(SessionSpecRow.Mapper.class)
    List<SessionSpecRow> getSessionSpecRows();

    @SqlQuery("SELECT tag_spec_id, client_tag FROM " + CLIENT_TAGS_TABLE)
    @UseRowMapper(ClientTagRow.Mapper.class)
    List<ClientTagRow> getClientTagRows();

    @SqlQuery("SELECT property_spec_id, session_property_name, session_property_value FROM " + PROPERTIES_TABLE)
    @UseRowMapper(SessionPropertyRow.Mapper.class)
    List<SessionPropertyRow> getSessionPropertyRows();

    /**
     * Assembles {@link SessionMatchSpec}s from three ordered result sets rather than aggregating child rows in SQL.
     * This keeps the read path dialect-neutral and avoids joining/splitting on commas, so client tags and property
     * values that contain a comma round-trip correctly.
     */
    default List<SessionMatchSpec> getSessionMatchSpecs()
    {
        ImmutableListMultimap.Builder<Long, String> clientTags = ImmutableListMultimap.builder();
        for (ClientTagRow row : getClientTagRows()) {
            clientTags.put(row.specId(), row.clientTag());
        }
        ImmutableListMultimap<Long, String> tagsBySpecId = clientTags.build();

        ImmutableListMultimap.Builder<Long, SessionPropertyRow> properties = ImmutableListMultimap.builder();
        for (SessionPropertyRow row : getSessionPropertyRows()) {
            properties.put(row.specId(), row);
        }
        ImmutableListMultimap<Long, SessionPropertyRow> propertiesBySpecId = properties.build();

        ImmutableList.Builder<SessionMatchSpec> specs = ImmutableList.builder();
        for (SessionSpecRow spec : getSessionSpecRows()) {
            ImmutableMap.Builder<String, String> sessionProperties = ImmutableMap.builder();
            for (SessionPropertyRow property : propertiesBySpecId.get(spec.specId())) {
                // session_property_value is nullable in the schema; a property with no value cannot set anything,
                // so skip it rather than let a single malformed row fail the whole reload (which would keep stale specs).
                if (property.value() != null) {
                    sessionProperties.put(property.name(), property.value());
                }
            }
            specs.add(new SessionMatchSpec(
                    Optional.ofNullable(spec.userRegex()).map(Pattern::compile),
                    Optional.ofNullable(spec.sourceRegex()).map(Pattern::compile),
                    Optional.of(tagsBySpecId.get(spec.specId())),
                    Optional.ofNullable(spec.queryType()),
                    Optional.ofNullable(spec.groupRegex()).map(Pattern::compile),
                    sessionProperties.buildOrThrow()));
        }
        return specs.build();
    }

    @VisibleForTesting
    @SqlUpdate("INSERT INTO " + SESSION_SPECS_TABLE + " (spec_id, user_regex, source_regex, query_type, group_regex, priority)\n" +
            "VALUES (:spec_id, :user_regex, :source_regex, :query_type, :group_regex, :priority)")
    void insertSpecRow(
            @Bind("spec_id") long specId,
            @Bind("user_regex") String userRegex,
            @Bind("source_regex") String sourceRegex,
            @Bind("query_type") String queryType,
            @Bind("group_regex") String groupRegex,
            @Bind("priority") int priority);

    @VisibleForTesting
    @SqlUpdate("INSERT INTO " + CLIENT_TAGS_TABLE + " (tag_spec_id, client_tag) VALUES (:spec_id, :client_tag)")
    void insertClientTag(@Bind("spec_id") long specId, @Bind("client_tag") String clientTag);

    @VisibleForTesting
    @SqlUpdate("INSERT INTO " + PROPERTIES_TABLE + " (property_spec_id, session_property_name, session_property_value)\n" +
            "VALUES (:property_spec_id, :session_property_name, :session_property_value)")
    void insertSessionProperty(
            @Bind("property_spec_id") long propertySpecId,
            @Bind("session_property_name") String sessionPropertyName,
            @Bind("session_property_value") String sessionPropertyValue);

    record SessionSpecRow(long specId, String userRegex, String sourceRegex, String queryType, String groupRegex, int priority)
    {
        public static class Mapper
                implements RowMapper<SessionSpecRow>
        {
            @Override
            public SessionSpecRow map(ResultSet resultSet, StatementContext context)
                    throws SQLException
            {
                return new SessionSpecRow(
                        resultSet.getLong("spec_id"),
                        resultSet.getString("user_regex"),
                        resultSet.getString("source_regex"),
                        resultSet.getString("query_type"),
                        resultSet.getString("group_regex"),
                        resultSet.getInt("priority"));
            }
        }
    }

    record ClientTagRow(long specId, String clientTag)
    {
        public static class Mapper
                implements RowMapper<ClientTagRow>
        {
            @Override
            public ClientTagRow map(ResultSet resultSet, StatementContext context)
                    throws SQLException
            {
                return new ClientTagRow(resultSet.getLong("tag_spec_id"), resultSet.getString("client_tag"));
            }
        }
    }

    record SessionPropertyRow(long specId, String name, String value)
    {
        public static class Mapper
                implements RowMapper<SessionPropertyRow>
        {
            @Override
            public SessionPropertyRow map(ResultSet resultSet, StatementContext context)
                    throws SQLException
            {
                return new SessionPropertyRow(
                        resultSet.getLong("property_spec_id"),
                        resultSet.getString("session_property_name"),
                        resultSet.getString("session_property_value"));
            }
        }
    }
}
