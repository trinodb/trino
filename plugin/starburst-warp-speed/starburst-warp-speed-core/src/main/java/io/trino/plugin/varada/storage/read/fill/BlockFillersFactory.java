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
package io.trino.plugin.varada.storage.read.fill;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.gen.constants.RecTypeCode;

@Singleton
public class BlockFillersFactory
{
    private final BlockFiller[] recTypeCodeToBlockFiller;

    @Inject
    public BlockFillersFactory(
            StorageEngineConstants storageEngineConstants,
            NativeConfiguration nativeConfiguration)
    {
        recTypeCodeToBlockFiller = new BlockFiller[RecTypeCode.REC_TYPE_NUM_OF.ordinal() + 1];
        for (RecTypeCode recTypeCode : RecTypeCode.values()) {
            recTypeCodeToBlockFiller[recTypeCode.ordinal()] = switch (recTypeCode) {
                case REC_TYPE_BOOLEAN -> new BooleanBlockFiller();
                case REC_TYPE_TIMESTAMP, REC_TYPE_TIMESTAMP_WITH_TZ, REC_TYPE_TIME, REC_TYPE_BIGINT, REC_TYPE_DECIMAL_SHORT, REC_TYPE_DOUBLE ->
                        new LongBlockFiller();
                case REC_TYPE_INTEGER, REC_TYPE_REAL, REC_TYPE_DATE -> new IntBlockFiller();
                case REC_TYPE_SMALLINT -> new ShortBlockFiller();
                case REC_TYPE_TINYINT -> new TinyIntBlockFiller();
                case REC_TYPE_DECIMAL_LONG -> new LongDecimalBlockFiller();
                case REC_TYPE_CHAR -> new FixedLengthStringSliceBlockFiller(storageEngineConstants, nativeConfiguration);
                case REC_TYPE_VARCHAR -> new VariableLengthStringSliceBlockFiller(storageEngineConstants, nativeConfiguration);
                case REC_TYPE_ARRAY_INT -> new IntArrayBlockFiller(storageEngineConstants);
                case REC_TYPE_ARRAY_BIGINT, REC_TYPE_ARRAY_DOUBLE -> new BigIntArrayBlockFiller(storageEngineConstants);
                case REC_TYPE_ARRAY_VARCHAR -> new VarcharArrayBlockFiller(storageEngineConstants);
                case REC_TYPE_ARRAY_CHAR -> new CharArrayBlockFiller(storageEngineConstants);
                case REC_TYPE_ARRAY_BOOLEAN -> new BooleanArrayBlockFiller(storageEngineConstants);
                default -> null;
            };
        }
    }

    public BlockFiller getBlockFiller(int recTypeCode)
    {
        return recTypeCodeToBlockFiller[recTypeCode];
    }
}
