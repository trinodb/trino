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
package io.trino.plugin.neo4j;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.connector.RecordCursor;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class Neo4jRecordCursorBase
        implements RecordCursor
{
    private static final Logger log = Logger.get(Neo4jRecordCursorBase.class);

    protected final Session session;
    protected final Neo4jTypeManager typeManager;
    private final String cypher;

    protected final Result result;
    protected Record record;
    private long readTimeNanos;

    public Neo4jRecordCursorBase(
            Neo4jClient client,
            Neo4jTypeManager typeManager,
            Optional<String> databaseName,
            String cypher)
    {
        this.session = requireNonNull(client.newSession(databaseName), "session is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cypher = requireNonNull(cypher, "cypher is null");

        log.debug("Executing: %s", cypher);
        this.result = this.session.run(cypher);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return this.readTimeNanos;
    }

    @Override
    public boolean advanceNextPosition()
    {
        long start = System.nanoTime();

        boolean hasNext = this.result.hasNext();

        if (hasNext) {
            this.record = this.result.next();
        }

        this.readTimeNanos += (System.nanoTime() - start);

        return hasNext;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new IllegalStateException("Unexpected call to getBoolean(field)");
    }

    @Override
    public long getLong(int field)
    {
        throw new IllegalStateException("Unexpected call to getLong(field)");
    }

    @Override
    public double getDouble(int field)
    {
        throw new IllegalStateException("Unexpected call to getDouble(field)");
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new IllegalStateException("Unexpected call to getSlice(field)");
    }

    @Override
    public Object getObject(int field)
    {
        throw new IllegalStateException("Unexpected call to getObject(field)");
    }

    @Override
    public boolean isNull(int field)
    {
        throw new IllegalStateException("Unexpected call to isNull(field)");
    }

    @Override
    public void close()
    {
        if (this.session != null) {
            this.session.close();
        }
    }
}
