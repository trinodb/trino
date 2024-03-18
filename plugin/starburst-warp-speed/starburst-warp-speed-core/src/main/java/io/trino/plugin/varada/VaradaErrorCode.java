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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.plugin.varada.util.TrinoExceptionMapper.VARADA_ERROR_CODE_OFFSET;
import static io.trino.spi.ErrorType.EXTERNAL;

public enum VaradaErrorCode
        implements ErrorCodeSupplier
{
    VARADA_GENERIC(0, EXTERNAL),
    VARADA_SETUP(1, EXTERNAL),
    VARADA_CONTROL(3, EXTERNAL),
    VARADA_ILLEGAL_PARAMETER(5, EXTERNAL),

    // Column/Partition
    VARADA_COLUMN_UNKNOWN_PARTITION_COLUMN_TYPE(315, EXTERNAL),

    //    Coordinator/Worker/Nodes
    VARADA_CLUSTER_NOT_READY(406, EXTERNAL),
    VARADA_DUPLICATE_RECORD(407, EXTERNAL),
    VARADA_RULE_CONFIGURATION_ERROR(408, EXTERNAL),
    VARADA_EXCEEDED_LOADERS(410, ErrorType.INSUFFICIENT_RESOURCES),

    // storage engine
    VARADA_UNRECOVERABLE_COLLECT_FAILED(502, EXTERNAL),
    VARADA_ROW_GROUP_ILLEGAL_STATE(505, EXTERNAL),
    VARADA_NATIVE_READ_OUT_OF_BOUNDS(506, EXTERNAL),
    VARADA_STORAGE_TEMPORARY_ERROR(507, EXTERNAL),
    VARADA_STORAGE_PERMANENT_ERROR(508, EXTERNAL),
    VARADA_NATIVE_ERROR(509, EXTERNAL),
    VARADA_NATIVE_UNRECOVERABLE_ERROR(510, EXTERNAL),
    VARADA_UNRECOVERABLE_MATCH_FAILED(511, EXTERNAL),
    VARADA_ILLEGAL_BUCKET_CONFIGURATION(512, EXTERNAL),
    VARADA_MATCH_FAILED(513, EXTERNAL),
    VARADA_FAILED_TO_BUILD_MIXED_PAGE(514, EXTERNAL),
    VARADA_MATCH_RANGES_ERROR(515, EXTERNAL),
    VARADA_TX_ALLOCATION_FAILED(516, EXTERNAL),
    VARADA_TX_ALLOCATION_INTERRUPTED(517, EXTERNAL),
    VARADA_PREDICATE_BUFFER_ALLOCATION(518, EXTERNAL),
    VARADA_MATCH_COLLECT_ID_ALLOCATION(522, EXTERNAL),

    // WARMUP RULES
    VARADA_WARMUP_RULE_BLOOM_ILLEGAL_TYPE(600, EXTERNAL),
    VARADA_WARMUP_RULE_BLOOM_TOO_MANY_BLOOM_WARMUPS(601, EXTERNAL),
    VARADA_WARMUP_RULE_UNKNOWN_TABLE(602, EXTERNAL),
    VARADA_WARMUP_RULE_UNKNOWN_COLUMN(603, EXTERNAL),
    VARADA_WARMUP_RULE_ILLEGAL_CHAR_LENGTH(604, EXTERNAL),
    VARADA_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE(605, EXTERNAL),
    VARADA_PREDICATE_CACHE_ERROR(606, EXTERNAL),
    VARADA_LUCENE_WRITER_ERROR(607, EXTERNAL),
    VARADA_LUCENE_FAILURE(608, EXTERNAL),
    VARADA_DATA_WARMUP_RULE_ILLEGAL_CHAR_LENGTH(609, EXTERNAL),

    VARADA_WARMUP_RULE_ID_NOT_VALID(610, EXTERNAL),
    VARADA_INDEX_WARMUP_RULE_IS_NOT_ALLOWED(611, EXTERNAL),
    VARADA_WARMUP_OPEN_ERROR(612, EXTERNAL),

    //dictionary
    VARADA_DICTIONARY_ERROR(800, EXTERNAL);

    private final ErrorType type;
    private final int code;

    VaradaErrorCode(int code, ErrorType type)
    {
        this.type = type;
        this.code = code;
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return new ErrorCode(VARADA_ERROR_CODE_OFFSET + code, name(), type);
    }

    public int getCode()
    {
        return code;
    }
}
