
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

package io.trino.plugin.warp.gen.constants;

public enum RecTypeCode
{
    REC_TYPE_INVALID(false, false),
    REC_TYPE_BOOLEAN(false, false),
    REC_TYPE_TINYINT(false, true),
    REC_TYPE_SMALLINT(false, true),
    REC_TYPE_INTEGER(true, true),
    REC_TYPE_DATE(false, true),
    REC_TYPE_REAL(false, true),
    REC_TYPE_BIGINT(true, true),
    REC_TYPE_DOUBLE(false, true),
    REC_TYPE_TIMESTAMP(false, true),
    REC_TYPE_TIMESTAMP_WITH_TZ(false, false),
    REC_TYPE_TIME(false, true),
    REC_TYPE_DECIMAL_SHORT(true, false),
    REC_TYPE_DECIMAL_LONG(false, false),
    REC_TYPE_CHAR(true, true),
    REC_TYPE_VARCHAR(true, true),
    REC_TYPE_ARRAY_INT(false, false),
    REC_TYPE_ARRAY_BIGINT(false, false),
    REC_TYPE_ARRAY_VARCHAR(false, false),
    REC_TYPE_ARRAY_CHAR(false, false),
    REC_TYPE_ARRAY_BOOLEAN(false, false),
    REC_TYPE_ARRAY_TIMESTAMP(false, false),
    REC_TYPE_ARRAY_DOUBLE(false, false),
    REC_TYPE_ARRAY_DATE(false, false),
    REC_TYPE_VARBINARY(false, false),
    REC_TYPE_NUM_OF(false, false);
    final boolean isSupportedDictionary;
    final boolean isSupportedFiltering;

    RecTypeCode(boolean isSupportedDictionary, boolean isSupportedFiltering)
    {
        this.isSupportedDictionary = isSupportedDictionary;
        this.isSupportedFiltering = isSupportedFiltering;
    }

    public boolean isSupportedDictionary()
    {
        return isSupportedDictionary;
    }

    public boolean isSupportedFiltering()
    {
        return isSupportedFiltering;
    }
}
