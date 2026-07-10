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
package io.trino.server;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.util.LicenseUtils;
import io.trino.util.TimeUtils;

import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.LICENSE_EXPIRED;
import static io.trino.spi.StandardErrorCode.LICENSE_NOT_FOUND;

public class Authenticator
{
    private static final Logger LOG = Logger.get(Authenticator.class);

    private final boolean isCoordinator;

    @Inject
    public Authenticator(ServerConfig serverConfig)
    {
        this.isCoordinator = serverConfig.isCoordinator();
    }

    public void loadLicense()
    {
        if (isCoordinator) {
            LOG.info("-- Loading License --");
            if (!LicenseUtils.exists()) {
                throw new TrinoException(LICENSE_NOT_FOUND,
                        "License doesn't exist, check license path please.");
            }
            else if (!LicenseUtils.effective()) {
                throw new TrinoException(LICENSE_EXPIRED,
                        "License is expired, update license please.");
            }
            else if (LicenseUtils.expire(TimeUtils.WEEK)) {
                try {
                    for (int i = 1; i <= 3; i++) {
                        LOG.warn("WARNING: The license of trino will expired in 7 days!");
                        TimeUnit.SECONDS.sleep(3);
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
