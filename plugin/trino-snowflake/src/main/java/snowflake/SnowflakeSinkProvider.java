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

import com.google.inject.Inject;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSink;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.snowflake.SnowflakeSessionPropertiesProvider.WriteFormat;

public class SnowflakeSinkProvider
        implements ConnectorPageSinkProvider
{
    private final ConnectionFactory connectionFactory;
    private final JdbcClient jdbcClient;
    private final RemoteQueryModifier modifier;
    private final SnowflakeConfig config;
    private final SnowflakeTableProperties propertiesProvider;

    @Inject
    public SnowflakeSinkProvider(ConnectionFactory connectionFactory,
            JdbcClient jdbcClient,
            RemoteQueryModifier modifier,
            SnowflakeConfig config,
            SnowflakeTableProperties propertiesProvider)
    {
        System.out.println("CONSTRUCT CALLED ");
        this.connectionFactory = connectionFactory;
        this.jdbcClient = jdbcClient;
        this.modifier = modifier;
        this.config = config;
        this.propertiesProvider = propertiesProvider;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session, ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        System.out.println("CALLED CREATE TABLE PAGE SINK ");
        return write(session, (JdbcOutputTableHandle) outputTableHandle, pageSinkId);
    }

    private ConnectorPageSink write(ConnectorSession session, JdbcOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        System.out.println(outputTableHandle.getClass());
        Optional<List<JdbcTypeHandle>> jdbcColumnTypes = outputTableHandle.getJdbcColumnTypes();
        if (jdbcColumnTypes.isEmpty()) {
            //most linkly these writes are not heavy and trino can handle better;
            return new JdbcPageSink(session, outputTableHandle,
                    jdbcClient,
                    pageSinkId, modifier);
        }
        List<JdbcTypeHandle> jdbcTypeHandles = jdbcColumnTypes.get();
        List<Type> trinoTypes = JdbcTypeToTrinoType.parse(jdbcTypeHandles);
        WriteFormat format = propertiesProvider.getWriteFormat(session);
        System.out.println("FORMAT "+format);
        //if (format.equals(WriteFormat.JDBC)) {
            return new JdbcPageSink(session, outputTableHandle,
                    jdbcClient,
                    pageSinkId, modifier);
        //}
        /*else {
            return switch (format) {
                case JSON -> new SnowflakeJsonPageSink();
                case PARQUET -> new SnowflakeParquetPageSink(outputTableHandle.getColumnNames(),
                        trinoTypes);
                default -> throw new IllegalStateException("Unexpected value: " + format);
            };
        }*/
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        System.out.println("CALLED INSERT PAGE SINK ");
        return write(session, (JdbcOutputTableHandle) tableHandle, pageSinkId);
    }
}
