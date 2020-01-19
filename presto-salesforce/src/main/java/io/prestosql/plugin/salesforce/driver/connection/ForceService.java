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
package io.prestosql.plugin.salesforce.driver.connection;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import io.prestosql.plugin.salesforce.driver.oauth.ForceOAuthClient;
import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.concurrent.TimeUnit;

public final class ForceService
{
    public static final int EXPIRE_AFTER_CREATE = 60;
    public static final int EXPIRE_STORE_SIZE = 16;
    private static final String DEFAULT_LOGIN_DOMAIN = "login.salesforce.com";
    private static final String SANDBOX_LOGIN_DOMAIN = "test.salesforce.com";
    private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    private static final long READ_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    private static final String DEFAULT_API_VERSION = "43.0";
    private static final DB cacheDb = DBMaker.tempFileDB().closeOnJvmShutdown().make();

    private static HTreeMap<String, String> partnerUrlCache = cacheDb.hashMap("PartnerUrlCache", Serializer.STRING, Serializer.STRING).expireAfterCreate(EXPIRE_AFTER_CREATE, TimeUnit.MINUTES).expireStoreSize(EXPIRE_STORE_SIZE * FileUtils.ONE_MB).create();

    private ForceService() {}

    private static String getPartnerUrl(String accessToken, boolean sandbox)
    {
        return partnerUrlCache.computeIfAbsent(accessToken, s -> getPartnerUrlFromUserInfo(accessToken, sandbox));
    }

    private static String getPartnerUrlFromUserInfo(String accessToken, boolean sandbox)
    {
        return new ForceOAuthClient(CONNECTION_TIMEOUT, READ_TIMEOUT).getUserInfo(accessToken, sandbox).getPartnerUrl();
    }

    public static PartnerConnection createPartnerConnection(ForceConnectionInfo info)
            throws ConnectionException
    {
        return info.getSessionId() != null ? createConnectionBySessionId(info) : createConnectionByUserCredential(info);
    }

    private static PartnerConnection createConnectionBySessionId(ForceConnectionInfo info)
            throws ConnectionException
    {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setSessionId(info.getSessionId());

        if (info.getSandbox() != null) {
            partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(info.getSessionId(), info.getSandbox()));
            return Connector.newConnection(partnerConfig);
        }

        try {
            partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(info.getSessionId(), false));
            return Connector.newConnection(partnerConfig);
        }
        catch (RuntimeException re) {
            try {
                partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(info.getSessionId(), true));
                return Connector.newConnection(partnerConfig);
            }
            catch (RuntimeException r) {
                throw new ConnectionException(r.getMessage());
            }
        }
    }

    private static PartnerConnection createConnectionByUserCredential(ForceConnectionInfo info)
            throws ConnectionException
    {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(info.getUserName());

        if (info.getSecurityToken() != null) {
            partnerConfig.setPassword(info.getPassword() + info.getSecurityToken());
        }
        else {
            partnerConfig.setPassword(info.getPassword());
        }

        if (info.getSandbox() != null) {
            partnerConfig.setAuthEndpoint(buildAuthEndpoint(info.getSandbox()));
            return Connector.newConnection(partnerConfig);
        }

        try {
            partnerConfig.setAuthEndpoint(buildAuthEndpoint(false));
            return Connector.newConnection(partnerConfig);
        }
        catch (ConnectionException ce) {
            partnerConfig.setAuthEndpoint(buildAuthEndpoint(true));
            return Connector.newConnection(partnerConfig);
        }
    }

    private static String buildAuthEndpoint(boolean sandbox)
    {
        if (sandbox) {
            return String.format("https://%s/services/Soap/u/%s", SANDBOX_LOGIN_DOMAIN, DEFAULT_API_VERSION);
        }
        else {
            return String.format("https://%s/services/Soap/u/%s", DEFAULT_LOGIN_DOMAIN, DEFAULT_API_VERSION);
        }
    }
}
