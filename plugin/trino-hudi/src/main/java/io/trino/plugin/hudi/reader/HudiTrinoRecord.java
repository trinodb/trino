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
package io.trino.plugin.hudi.reader;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HudiTrinoRecord
        extends HoodieRecord<IndexedRecord>
{
    public HudiTrinoRecord()
    {
    }

    @Override
    public HoodieRecord<IndexedRecord> newInstance()
    {
        return null;
    }

    @Override
    public HoodieRecord<IndexedRecord> newInstance(HoodieKey hoodieKey, HoodieOperation hoodieOperation)
    {
        return null;
    }

    @Override
    public HoodieRecord<IndexedRecord> newInstance(HoodieKey hoodieKey)
    {
        return null;
    }

    @Override
    public Comparable<?> doGetOrderingValue(Schema schema, Properties properties)
    {
        return null;
    }

    @Override
    public HoodieRecordType getRecordType()
    {
        return null;
    }

    @Override
    public String getRecordKey(Schema schema, Option<BaseKeyGenerator> option)
    {
        return "";
    }

    @Override
    public String getRecordKey(Schema schema, String s)
    {
        return "";
    }

    @Override
    protected void writeRecordPayload(IndexedRecord page, Kryo kryo, Output output)
    {
    }

    @Override
    protected IndexedRecord readRecordPayload(Kryo kryo, Input input)
    {
        return null;
    }

    @Override
    public Object[] getColumnValues(Schema schema, String[] strings, boolean b)
    {
        return new Object[0];
    }

    @Override
    public HoodieRecord joinWith(HoodieRecord hoodieRecord, Schema schema)
    {
        return null;
    }

    @Override
    public HoodieRecord prependMetaFields(Schema schema, Schema schema1,
            MetadataValues metadataValues, Properties properties)
    {
        return null;
    }

    @Override
    public HoodieRecord rewriteRecordWithNewSchema(Schema schema, Properties properties,
            Schema schema1, Map<String, String> map)
    {
        return null;
    }

    @Override
    public boolean isDelete(Schema schema, Properties properties)
            throws IOException
    {
        return false;
    }

    @Override
    public boolean shouldIgnore(Schema schema, Properties properties)
            throws IOException
    {
        return false;
    }

    @Override
    public HoodieRecord<IndexedRecord> copy()
    {
        return null;
    }

    @Override
    public Option<Map<String, String>> getMetadata()
    {
        return null;
    }

    @Override
    public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema schema, Properties properties,
            Option<Pair<String, String>> option, Boolean aBoolean, Option<String> option1,
            Boolean aBoolean1, Option<Schema> option2)
            throws IOException
    {
        return null;
    }

    @Override
    public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema schema, Properties properties,
            Option<BaseKeyGenerator> option)
    {
        return null;
    }

    @Override
    public HoodieRecord truncateRecordKey(Schema schema, Properties properties, String s)
            throws IOException
    {
        return null;
    }

    @Override
    public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema schema, Properties properties)
            throws IOException
    {
        return null;
    }

    @Override
    public ByteArrayOutputStream getAvroBytes(Schema schema, Properties properties)
            throws IOException
    {
        return null;
    }
}
