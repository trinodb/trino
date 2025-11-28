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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.types.RowType;

final class SimpleTableTestHelper
{
    private static final String USER = "user";

    private final InnerTableWrite writer;
    private final InnerTableCommit commit;
    private final FileStoreTable table;

    public SimpleTableTestHelper(Path path, RowType rowType)
            throws Exception
    {
        Schema schema = new Schema(rowType.getFields(), ImmutableList.of(), ImmutableList.of("a"), ImmutableMap.of("bucket", "1"), "");
        new SchemaManager(LocalFileIO.create(), path).createTable(schema);
        table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        this.writer = table.newWrite(USER);
        this.commit = table.newCommit(USER);
    }

    public void write(InternalRow row)
            throws Exception
    {
        writer.write(row);
    }

    public void createTag(String name)
    {
        table.createTag(name);
    }

    public void commit()
            throws Exception
    {
        commit.commit(0, writer.prepareCommit(true, 0));
    }
}
