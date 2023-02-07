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
package snowflake;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;
import java.util.List;

import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class SnowflakeTableProperties implements TablePropertiesProvider
{
    public static final String WRITE_FORMAT = "snowflake_write_format";
    private final ImmutableList<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SnowflakeTableProperties(SnowflakeConfig config)
    {
        System.out.println("CALLED TABLE PROPERTIES ");
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        WRITE_FORMAT,
                        "Format in which data will be written on to snowflake stage location." +
                                "Possible values are PARQUET,JSON,JDBC.\n" +
                                "Using PARQUET or JSON as write format writes files over to the s3 stage location " +
                                "and uses a Snowflake COPY COMMAND to write data to the table.\n" +
                                "Using JDBC format means all data will be written to snowflake over a connection," +
                                " this may impact performance of trino writes.",
                        SnowflakeSessionPropertiesProvider.WriteFormat.class,
                        SnowflakeSessionPropertiesProvider.WriteFormat.JDBC,
                        false))
                .build();
    }

    public SnowflakeSessionPropertiesProvider.WriteFormat getWriteFormat(ConnectorSession session)
    {
        return session.getProperty(WRITE_FORMAT, SnowflakeSessionPropertiesProvider.WriteFormat.class);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return sessionProperties;
    }

    public enum WriteFormat
    {
        PARQUET, JSON, JDBC
    }
}
