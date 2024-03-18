
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

package io.trino.plugin.warp.gen.errorcodes;

public enum ErrorCodes {
    ENV_EXCEPTION_UNRECOVERABLE(0, "native storage engine unrecoverable error", true),
    ENV_EXCEPTION_INITIALIZATION(1, "native storage engine failed to initialize", false),
    ENV_EXCEPTION_WRITE_FLOW_ERROR(2, "native storage engine: failure in data write flow", false),
    ENV_EXCEPTION_READ_FLOW_ERROR(3, "native storage engine: failure in data read flow", true),
    ENV_EXCEPTION_UNKNOWN_TYPE(4, "native storage engine: unknown type in switch", true),
    ENV_EXCEPTION_ALLOC_FAILED(5, "native storage engine: failed to allocate memory", false),
    ENV_EXCEPTION_ILLEGAL_PARAMS(6, "native storage engine: illegal parameters passed to method", true),
    ENV_EXCEPTION_ILLEGAL_STATE(7, "native storage engine: illegal state reached", true),
    ENV_EXCEPTION_HA_ERROR(8, "native storage engine: max submissions reached error", false),
    ENV_EXCEPTION_CACHE_ERROR(9, "native storage engine: cache error", false),
    ENV_EXCEPTION_IO_ERROR(10, "native storage engine: error in io path", false),
    ENV_EXCEPTION_STORAGE_CFG_ERROR(11, "native storage engine: error in storage layer config", false),
    ENV_EXCEPTION_NO_RESOURCES(12, "native storage engine: lack of resources", false),
    ENV_EXCEPTION_STORAGE_ERROR(13, "native storage engine: aborted tx called allocate or free storage page", false),
    ENV_EXCEPTION_LUCENE_ERROR(14, "native storage engine: lucene match error", false),
    ENV_EXCEPTION_INLINE_ERROR(15, "native storage engine: inline error", false),
    ENV_EXCEPTION_BLOOM_ERROR(16, "native storage engine: bloom error", false),
    ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR(17, "native storage engine: temporary error in storage layer", false),
    ENV_EXCEPTION_STORAGE_PERMANENT_ERROR(18, "native storage engine: permanent error in storage layer", false),
    ENV_EXCEPTION_STORAGE_TIMEOUT_ERROR(19, "native storage engine: timeout in storage layer", false),
    ENV_EXCEPTION_STORAGE_READ_OUT_OF_BOUNDS(20, "native storage engine: read out of bounds", false),
    ENV_EXCEPTION_ALWAYS_THROW(100, "native storage engine: always throw min code, should not be used directly", false),
    ENV_EXCEPTION_OUT_OF_SPACE(101, "native storage engine: out of disk space", false),
    ENV_EXCEPTION_LUCENE_FILE_TOO_BIG(102, "native storage engine: lucene file is too big", false),
    ENV_EXCEPTION_WARM_FILE_TOO_MANY_ROWS(103, "native storage engine: warm file has too many rows", false),
    ENV_EXCEPTION_FAILED_TO_INIT_LIBAIO(104, "native storage engine: aio_max_nr was not set properly", true),
    ENV_EXCEPTION_DISK_ACCESS_PERMISSONS(105, "native storage engine: failed in file system operation (open/close/etc) probably due to disk access permissions", true);

    private final int code;
    private final String message;
    private final boolean unrecoverable;

    ErrorCodes(int code, String message, boolean unrecoverable)
    {
        this.code = code;
        this.message = message;
        this.unrecoverable = unrecoverable;
    }

    public int getCode()
    {
        return this.code;
    }

    public String getMessage()
    {
        return this.message;
    }

    public boolean getUnrecoverable()
    {
        return this.unrecoverable;
    }
}
