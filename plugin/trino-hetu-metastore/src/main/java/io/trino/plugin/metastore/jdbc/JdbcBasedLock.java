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
package io.trino.plugin.metastore.jdbc;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.Jdbi;

import static io.trino.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static java.lang.String.format;

public class JdbcBasedLock
{
    private static final Logger LOG = Logger.get(JdbcBasedLock.class);

    public static final long LOCK_RETRY_COUNT = 300;
    public static final long LOCK_TIMEOUT_COUNT = 30;
    public static final long RETRY_INTERVAL = 1000;
    private static final int UNLOCK_RETRY_COUNT = 3;

    private final JdbcMetadataDao dao;

    public JdbcBasedLock(Jdbi jdbi)
    {
        this.dao = JdbcMetadataUtil.onDemand(jdbi, JdbcMetadataDao.class);
    }

    public void lock()
    {
        int count = 1;
        int lockCount = 1;
        Long lockId = Long.MIN_VALUE;
        while (count <= LOCK_RETRY_COUNT) {
            try {
                dao.tryLock();
                return;
            }
            catch (TrinoException e) {
                // retry timeout
                if (count == LOCK_RETRY_COUNT) {
                    LOG.warn(format("After reaching the maximum %s retries, get lock failed.", LOCK_RETRY_COUNT));
                    throw new TrinoException(HETU_METASTORE_CODE,
                            format("After reaching the maximum %s retries, get lock failed.", LOCK_RETRY_COUNT), e);
                }

                LOG.debug("Failed to get lock. Will retry again in %s milliseconds. Exception: %s", RETRY_INTERVAL, e);
                try {
                    Thread.sleep(RETRY_INTERVAL);
                }
                catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }

                // check lock expired
                Long id = dao.getLockId();
                if (id == null) {
                    lockCount = 1;
                }
                else if (id > lockId) {
                    lockId = id;
                    lockCount = 1;
                }
                else if (lockCount > LOCK_TIMEOUT_COUNT) {
                    unlock();
                }
            }

            count++;
            lockCount++;
        }
    }

    public void unlock()
    {
        int count = 1;
        while (count <= UNLOCK_RETRY_COUNT) {
            try {
                dao.releaseLock();
                return;
            }
            catch (TrinoException e) {
                if (count == UNLOCK_RETRY_COUNT) {
                    throw new TrinoException(HETU_METASTORE_CODE, e.getMessage(), e);
                }

                LOG.debug("Failed to release lock. Will retry again in %s milliseconds. Exception: %s", RETRY_INTERVAL, e);
                try {
                    Thread.sleep(RETRY_INTERVAL);
                }
                catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
                count++;
            }
        }
    }
}
