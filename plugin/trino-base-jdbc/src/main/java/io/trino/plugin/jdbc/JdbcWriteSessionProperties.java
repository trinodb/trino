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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.plugin.jdbc.JdbcWriteConfig.MAX_ALLOWED_WRITE_BATCH_SIZE;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static java.lang.String.format;

public class JdbcWriteSessionProperties
        implements SessionPropertiesProvider
{
    public static final String WRITE_BATCH_SIZE = "write_batch_size";
    public static final String NON_TRANSACTIONAL_INSERT = "non_transactional_insert";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public JdbcWriteSessionProperties(JdbcWriteConfig writeConfig)
    {
        properties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(integerProperty(
                        WRITE_BATCH_SIZE,
                        "Maximum number of rows to write in a single batch",
                        writeConfig.getWriteBatchSize(),
                        JdbcWriteSessionProperties::validateWriteBatchSize,
                        false))
                .add(booleanProperty(
                        NON_TRANSACTIONAL_INSERT,
                        "Do not use temporary table on insert to table",
                        writeConfig.isNonTransactionalInsert(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static int getWriteBatchSize(ConnectorSession session)
    {
        return session.getProperty(WRITE_BATCH_SIZE, Integer.class);
    }

    public static boolean isNonTransactionalInsert(ConnectorSession session)
    {
        return session.getProperty(NON_TRANSACTIONAL_INSERT, Boolean.class);
    }

    private static void validateWriteBatchSize(int maxBatchSize)
    {
        if (maxBatchSize < 1) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be greater than 0: %s", WRITE_BATCH_SIZE, maxBatchSize));
        }
        if (maxBatchSize > MAX_ALLOWED_WRITE_BATCH_SIZE) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s cannot exceed %s: %s", WRITE_BATCH_SIZE, MAX_ALLOWED_WRITE_BATCH_SIZE, maxBatchSize));
        }
    }
}
