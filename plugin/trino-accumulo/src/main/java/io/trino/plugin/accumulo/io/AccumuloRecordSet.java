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
package io.trino.plugin.accumulo.io;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.trino.plugin.accumulo.conf.AccumuloSessionProperties;
import io.trino.plugin.accumulo.model.AccumuloColumnHandle;
import io.trino.plugin.accumulo.model.AccumuloSplit;
import io.trino.plugin.accumulo.model.AccumuloTableHandle;
import io.trino.plugin.accumulo.serializers.AccumuloRowSerializer;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a Trino RecordSet, responsible for returning the column types and the RecordCursor to the framework.
 *
 * @see AccumuloRecordCursor
 * @see AccumuloRecordSetProvider
 */
public class AccumuloRecordSet
        implements RecordSet
{
    private static final Logger LOG = Logger.get(AccumuloRecordSet.class);
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private final List<AccumuloColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final AccumuloRowSerializer serializer;
    private final BatchScanner scanner;
    private final String rowIdName;

    public AccumuloRecordSet(
            Connector connector,
            ConnectorSession session,
            AccumuloSplit split,
            String username,
            AccumuloTableHandle table,
            List<AccumuloColumnHandle> columnHandles)
    {
        requireNonNull(session, "session is null");
        requireNonNull(split, "split is null");
        requireNonNull(username, "username is null");
        requireNonNull(table, "table is null");

        rowIdName = table.getRowId();

        serializer = table.getSerializerInstance();

        // Save off the column handles and createa list of the Accumulo types
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (AccumuloColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();

        try {
            // Create the BatchScanner and set the ranges from the split
            scanner = connector.createBatchScanner(table.getFullTableName(), getScanAuthorizations(session, table, connector, username), 10);
            scanner.setRanges(split.getRanges());
        }
        catch (Exception e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, format("Failed to create batch scanner for table %s", table.getFullTableName()), e);
        }
    }

    /**
     * Gets the scanner authorizations to use for scanning tables.
     * <p>
     * In order of priority: session username authorizations, then table property, then the default connector auths.
     *
     * @param session Current session
     * @param table Accumulo table
     * @param connector Accumulo connector
     * @param username Accumulo username
     * @return Scan authorizations
     * @throws AccumuloException If a generic Accumulo error occurs
     * @throws AccumuloSecurityException If a security exception occurs
     */
    private static Authorizations getScanAuthorizations(ConnectorSession session, AccumuloTableHandle table, Connector connector, String username)
            throws AccumuloException, AccumuloSecurityException
    {
        String sessionScanUser = AccumuloSessionProperties.getScanUsername(session);
        if (sessionScanUser != null) {
            Authorizations scanAuths = connector.securityOperations().getUserAuthorizations(sessionScanUser);
            LOG.debug("Using session scanner auths for user %s: %s", sessionScanUser, scanAuths);
            return scanAuths;
        }

        Optional<String> scanAuths = table.getScanAuthorizations();
        if (scanAuths.isPresent()) {
            Authorizations auths = new Authorizations(Iterables.toArray(COMMA_SPLITTER.split(scanAuths.get()), String.class));
            LOG.debug("scan_auths table property set: %s", auths);
            return auths;
        }
        Authorizations auths = connector.securityOperations().getUserAuthorizations(username);
        LOG.debug("scan_auths table property not set, using user auths: %s", auths);
        return auths;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new AccumuloRecordCursor(serializer, scanner, rowIdName, columnHandles);
    }
}
