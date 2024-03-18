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
package io.trino.plugin.varada.storage.write.dictionary;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.gen.constants.RecTypeCode;

@Singleton
public class DictionaryWriterFactory
{
    private final DictionaryWriter[] recTypeCodeToDictionaryWriter;

    @Inject
    public DictionaryWriterFactory()
    {
        recTypeCodeToDictionaryWriter = new DictionaryWriter[RecTypeCode.REC_TYPE_NUM_OF.ordinal() + 1];

        for (RecTypeCode recTypeCode : RecTypeCode.values()) {
            recTypeCodeToDictionaryWriter[recTypeCode.ordinal()] = switch (recTypeCode) {
                case REC_TYPE_TIMESTAMP, REC_TYPE_TIMESTAMP_WITH_TZ, REC_TYPE_TIME, REC_TYPE_BIGINT, REC_TYPE_DECIMAL_SHORT ->
                        new LongDictionaryWriter();
                case REC_TYPE_INTEGER, REC_TYPE_REAL, REC_TYPE_DATE -> new IntDictionaryWriter();
                case REC_TYPE_CHAR -> new FixedLengthStringDictionaryWriter();
                case REC_TYPE_VARCHAR -> new VariableLengthStringDictionaryWriter();
                default -> null;
            };
        }
    }

    public DictionaryWriter getDictionaryWriter(int recTypeCode)
    {
        return recTypeCodeToDictionaryWriter[recTypeCode];
    }
}
