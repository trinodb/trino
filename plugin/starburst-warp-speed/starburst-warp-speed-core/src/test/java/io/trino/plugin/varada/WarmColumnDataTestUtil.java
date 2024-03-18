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
package io.trino.plugin.varada;

import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.DictionaryInfo;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.RecordData;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;

public class WarmColumnDataTestUtil
{
    private WarmColumnDataTestUtil()
    {}

    public static WarmupElementWriteMetadata createWarmUpElementWithDictionary(RecordData recordData, WarmUpType warmUpType)
    {
        NodeManager nodeManager = mockNodeManager();
        DictionaryKey dictionaryKey = new DictionaryKey(recordData.schemaTableColumn(),
                nodeManager.getCurrentNode().getNodeIdentifier(),
                DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
        DictionaryInfo dictionaryInfo = new DictionaryInfo(dictionaryKey, DictionaryState.DICTIONARY_NOT_EXIST, recordData.recTypeLength(), DictionaryInfo.NO_OFFSET);
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warmUpType(warmUpType)
                .recTypeCode(recordData.recTypeCode())
                .recTypeLength(recordData.recTypeLength())
                .varadaColumn(recordData.schemaTableColumn().varadaColumn())
                .dictionaryInfo(dictionaryInfo)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
        return WarmupElementWriteMetadata.builder()
                .warmUpElement(warmUpElement)
                .connectorBlockIndex(0)
                .type(recordData.type())
                .schemaTableColumn(recordData.schemaTableColumn())
                .build();
    }

    public static RecordData generateRecordData(String columnName, Type type)
    {
        return generateRecordData(new RegularColumn(columnName), type);
    }

    public static RecordData generateRecordData(VaradaColumn varadaColumn, Type type)
    {
        int varcharMaxLen = 7 * 8192;
        int recTypeLength = TypeUtils.getTypeLength(type, varcharMaxLen);
        RecTypeCode recTypeCode = TypeUtils.convertToRecTypeCode(type, recTypeLength, 8);
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(new SchemaTableName("SCHEMA", "TABLE"), varadaColumn);

        return new RecordData(schemaTableColumn, type, recTypeCode, recTypeLength);
    }
}
