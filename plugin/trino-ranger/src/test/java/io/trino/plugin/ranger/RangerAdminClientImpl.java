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
package io.trino.plugin.ranger;

import org.apache.ranger.admin.client.AbstractRangerAdminClient;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;

public class RangerAdminClientImpl
        extends AbstractRangerAdminClient
{
    private static final String policiesFilepath = "/src/test/resources/trino-policies.json";

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis)
            throws Exception
    {
        String basedir = System.getProperty("basedir");

        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }

        byte[] policiesBytes = Files.readAllBytes(FileSystems.getDefault().getPath(basedir, policiesFilepath));

        ServicePolicies ret = gson.fromJson(new String(policiesBytes, Charset.defaultCharset()), ServicePolicies.class);
        RangerServiceDef serviceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef("trino");
        RangerServiceDef tagServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef("tag");

        ret.setServiceDef(serviceDef);

        if (ret.getTagPolicies() == null) {
            ret.setTagPolicies(new ServicePolicies.TagPolicies());
        }

        ret.getTagPolicies().setServiceDef(tagServiceDef);

        return ret;
    }
}
