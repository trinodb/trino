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
package com.aliyun.odps.cupid.trino;

import com.aliyun.odps.cupid.table.v1.reader.InputSplit;
import com.aliyun.odps.cupid.table.v1.reader.SplitReader;
import com.aliyun.odps.cupid.table.v1.reader.SplitReaderBuilder;
import com.aliyun.odps.data.ArrayRecord;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class OdpsRecordSet
        implements RecordSet
{
    private final List<OdpsColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final SplitReader<ArrayRecord> recordReader;
    private final InputSplit odpsInputSplit;
    private final boolean isZeroColumn;

    public OdpsRecordSet(OdpsSplit split, List<OdpsColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (OdpsColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();
        this.isZeroColumn = split.getIsZeroColumn();

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(split.getInputSplit()));
            ObjectInputStream in = new ObjectInputStream(bais);
            odpsInputSplit = (InputSplit) in.readObject();
            recordReader = new SplitReaderBuilder(odpsInputSplit)
                    .buildRecordReader();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new OdpsRecordCursor(odpsInputSplit, columnHandles, recordReader, isZeroColumn);
    }
}
