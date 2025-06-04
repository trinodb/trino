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
package io.trino.plugin.deltalake.cache;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.plugin.deltalake.DeltaLakeConfig;

import java.util.Optional;

import static io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess.STARBURST_META_DIR;
import static io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess.STATISTICS_META_DIR;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;

public class DeltaLakeCacheKeyProvider
        implements CacheKeyProvider
{
    private final boolean deltaLogCacheDisabled;

    @Inject
    public DeltaLakeCacheKeyProvider(DeltaLakeConfig deltaLakeConfig)
    {
        // Disabling the delta log folder caching is useful in those scenarios when Delta Tables are deleted and re-created,
        // and caching their _delta_log directories should be avoided.
        this.deltaLogCacheDisabled = deltaLakeConfig.isDeltaLogFileSystemCacheDisabled();
    }

    /**
     * Get the cache key of a TrinoInputFile. Returns Optional.empty() if the file is not cacheable.
     */
    @Override
    public Optional<String> getCacheKey(TrinoInputFile inputFile)
    {
        String path = inputFile.location().path();
        // Explicitly exclude the files in the _delta_log directory when deltaLogFileSystemCacheDisabled is set to true,
        // as they can change when the Delta Table is overwritten, https://github.com/trinodb/trino/issues/21451
        if (deltaLogCacheDisabled && path.contains("/" + TRANSACTION_LOG_DIRECTORY + "/")) {
            return Optional.empty();
        }
        if (path.endsWith(".trinoSchema") || path.contains("/.trinoPermissions/")) {
            // Needed to avoid caching files from FileHiveMetastore on coordinator during tests
            return Optional.empty();
        }
        // All files within _delta_log are immutable, except _last_checkpoint and Trino metadata, https://github.com/delta-io/delta/issues/1975
        if (path.endsWith("/_delta_log/_last_checkpoint")
                || path.contains("/" + STATISTICS_META_DIR + "/")
                || path.contains("/" + STARBURST_META_DIR + "/")) {
            return Optional.empty();
        }
        return Optional.of(path);
    }
}
