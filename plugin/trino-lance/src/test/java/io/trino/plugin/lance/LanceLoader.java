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
package io.trino.plugin.lance;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.WriteParams;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryStatusInfo;
import io.trino.client.ResultRows;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.util.DateTimeUtils.parseDate;

public class LanceLoader
        extends AbstractTestingTrinoClient<Void>
{
    private final String tablePath;
    private final RootAllocator allocator = new RootAllocator();

    public LanceLoader(TestingTrinoServer trinoServer, Session defaultSession, String tablePath)
    {
        super(trinoServer, defaultSession);
        this.tablePath = tablePath;
    }

    @Override
    protected ResultsSession<Void> getResultSession(Session session)
    {
        return new LanceLoadingSession();
    }

    private class LanceLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<Schema> schema = new AtomicReference<>();
        private final AtomicReference<List<Type>> types = new AtomicReference<>();
        private final AtomicReference<List<List<Object>>> data = new AtomicReference<>();

        @Override
        public void addResults(QueryStatusInfo statusInfo, ResultRows rows)
        {
            if (schema.get() == null && statusInfo.getColumns() != null) {
                types.set(statusInfo.getColumns().stream().map(LanceLoader.this::toTrinoType).collect(toImmutableList()));
                data.set(new ArrayList<>());
                schema.set(toArrowSchema(statusInfo.getColumns()));
            }

            if (rows.isNull()) {
                return;
            }

            checkState(schema.get() != null, "rows received without schema set");
            for (List<Object> row : rows) {
                data.get().add(row);
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema.get(), allocator)) {
                root.allocateNew();
                List<Field> fields = schema.get().getFields();
                List<FieldVector> fieldVectors = fields.stream().map(root::getVector).collect(toImmutableList());

                for (int i = 0; i < data.get().size(); i++) {
                    List<Object> row = data.get().get(i);
                    for (int filedIdx = 0; filedIdx < fields.size(); filedIdx++) {
                        Type fieldType = types.get().get(filedIdx);
                        Object value = row.get(filedIdx);
                        switch (fieldType) {
                            case IntegerType _ -> ((IntVector) fieldVectors.get(filedIdx)).setSafe(i, ((Number) value).intValue());
                            case BigintType _ -> ((BigIntVector) fieldVectors.get(filedIdx)).setSafe(i, ((Number) value).longValue());
                            case DoubleType _ -> ((Float8Vector) fieldVectors.get(filedIdx)).setSafe(i, ((Number) value).doubleValue());
                            case VarcharType _ -> ((VarCharVector) fieldVectors.get(filedIdx)).setSafe(i, ((String) value).getBytes(StandardCharsets.UTF_8));
                            case DateType _ -> ((DateDayVector) fieldVectors.get(filedIdx)).setSafe(i, parseDate((String) value));
                            default -> throw new IllegalStateException("Unsupported fieldType: " + fieldType);
                        }
                    }
                }
                root.setRowCount(data.get().size());

                try (ArrowReader reader = new SimpleArrowReader(root, allocator); ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
                    Data.exportArrayStream(allocator, reader, stream);
                    WriteParams writeParams = new WriteParams.Builder()
                            .withDataStorageVersion(WriteParams.LanceFileVersion.V2_1)
                            .build();
                    Dataset dataset = Dataset.create(allocator, stream, tablePath, writeParams);
                    dataset.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        }

        private static class SimpleArrowReader
                extends ArrowReader
        {
            private final VectorSchemaRoot root;
            private boolean batchLoaded;

            private SimpleArrowReader(VectorSchemaRoot root, BufferAllocator allocator)
            {
                super(allocator);
                this.root = root;
                batchLoaded = false;
            }

            @Override
            public boolean loadNextBatch()
            {
                if (!batchLoaded) {
                    batchLoaded = true;
                    return true;
                }
                return false;
            }

            @Override
            public long bytesRead()
            {
                return root.getFieldVectors().stream()
                        .mapToLong(FieldVector::getBufferSize)
                        .sum();
            }

            @Override
            protected void closeReadSource() {}

            @Override
            protected Schema readSchema()
            {
                return root.getSchema();
            }

            @Override
            public VectorSchemaRoot getVectorSchemaRoot()
            {
                return root;
            }
        }
    }

    private Schema toArrowSchema(List<Column> columns)
    {
        List<Field> fields = columns.stream().map(column -> new Field(column.getName(), FieldType.notNullable(toArrowType(column)), null)).collect(toImmutableList());
        return new Schema(fields);
    }

    private Type toTrinoType(Column column)
    {
        return getServer().getPlannerContext().getTypeManager().fromSqlType(column.getType());
    }

    private ArrowType toArrowType(Column column)
    {
        Type trinoType = toTrinoType(column);
        return switch (trinoType) {
            case IntegerType _ -> new ArrowType.Int(32, true);
            case BigintType _ -> new ArrowType.Int(64, true);
            case DoubleType _ -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DateType _ -> new ArrowType.Date(DateUnit.DAY);
            case VarcharType _ -> new ArrowType.Utf8();
            default -> throw new IllegalStateException("Unsupported type: " + trinoType);
        };
    }
}
