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
package io.trino.plugin.redshift;

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.plugin.redshift.RedshiftSessionProperties.isUnloadEnabled;
import static java.util.Objects.requireNonNull;

public class RedshiftUnloadJdbcQueryEventListener
        implements JdbcQueryEventListener
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final String unloadLocation;

    @Inject
    public RedshiftUnloadJdbcQueryEventListener(@FileSystemS3 TrinoFileSystemFactory fileSystemFactory, RedshiftConfig redshiftConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.unloadLocation = redshiftConfig.getUnloadLocation().orElseThrow();
    }

    @Override
    public void beginQuery(ConnectorSession session) {}

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        if (isUnloadEnabled(session)) {
            try {
                fileSystemFactory.create(session).deleteDirectory(Location.of(unloadLocation + "/" + session.getQueryId()));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
