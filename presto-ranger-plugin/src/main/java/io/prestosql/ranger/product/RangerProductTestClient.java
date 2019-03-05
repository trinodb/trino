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
package io.prestosql.ranger.product;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;

public class RangerProductTestClient
        implements RangerAdminClient
{
    private static final Logger LOG = LoggerFactory.getLogger(RangerProductTestClient.class);
    private static final String cacheFilename = "hive-policies.json";
    private static final String tagFilename = "hive-policies-tag.json";
    private Gson gson;

    public void init(String serviceName, String appId, String configPropertyPrefix)
    {
        LOG.info("Initializing " + RangerProductTestClient.class.getCanonicalName());
        Gson gson = null;
        try {
            gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        }
        catch (Throwable excp) {
            LOG.error("RangerAdminClientImpl: failed to create GsonBuilder object", excp);
        }
        this.gson = gson;
        LOG.info("Finished initializing " + RangerProductTestClient.class.getCanonicalName());
    }

    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis)
            throws Exception
    {
        InputStream in = RangerProductTestClient.class.getClassLoader().getResourceAsStream(cacheFilename);
        LOG.info("Loading polices from  " + cacheFilename);
        return gson.fromJson(IOUtils.toString(in), ServicePolicies.class);
    }

    public void grantAccess(GrantRevokeRequest request)
            throws Exception
    {
    }

    public void revokeAccess(GrantRevokeRequest request)
            throws Exception
    {
    }

    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis)
            throws Exception
    {
        InputStream in = RangerProductTestClient.class.getClassLoader().getResourceAsStream(tagFilename);
        LOG.info("Loading service tags from  " + tagFilename);
        return gson.fromJson(IOUtils.toString(in), ServiceTags.class);
    }

    public List<String> getTagTypes(String tagTypePattern)
            throws Exception
    {
        return null;
    }
}
