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
package io.prestosql.plugin.password.salesforce;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.log.Logger;

import javax.validation.constraints.NotNull;

public class SalesforceConfig
{
    private static final Logger log = Logger.get(SalesforceConfig.class);

    protected static final String LOGINURL = "https://login.salesforce.com/services/Soap/u/";
    protected static final String APIVERSION = "46.0";

    protected static final String LOGIN_SOAP_MESSAGE = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n" +
            "<env:Envelope xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"\n" +
            "xmlns:urn=\"urn:enterprise.soap.sforce.com\"\n" +
            "   xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "   xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\">\n" +
            " <env:Header>\n" +
            "     <urn:CallOptions>\n" +
            "       <urn:client>SfdcInsights</urn:client>\n" +
            "     </urn:CallOptions>\n" +
            " </env:Header>\n" +
            " <env:Body>\n" +
            "   <n1:login xmlns:n1=\"urn:partner.soap.sforce.com\">\n" +
            "     <n1:username>%s</n1:username>\n" +
            "     <n1:password>%s</n1:password>\n" +
            "   </n1:login>\n" +
            " </env:Body>\n" +
            "</env:Envelope>\n";

    public static final int MAX_EXPIRE = 3600;
    private String org;
    private int cacheSize = 4096;
    private int cacheExpireSeconds = 120;

    @NotNull
    public String getOrg()
    {
        return org;
    }

    @Config("salesforce.org")
    @ConfigDescription("Salesforce 18 Character OrgId.")
    public SalesforceConfig setOrg(String org)
    {
        this.org = org;
        return this;
    }

    public int getCacheSize()
    {
        return cacheSize;
    }

    @Config("salesforce.cache-size")
    @ConfigDescription("Maximum size of the cache that holds authenticated users.")
    public SalesforceConfig setCacheSize(int cacheSize)
    {
        this.cacheSize = cacheSize;
        return this;
    }

    public int getCacheExpireSeconds()
    {
        return cacheExpireSeconds;
    }

    @Config("salesforce.cache-expire-seconds")
    @ConfigDescription("Expire time in minutes for an entry in cache since last write.  Max is " + MAX_EXPIRE + ".")
    public SalesforceConfig setCacheExpireSeconds(int cacheExpireSeconds)
    {
        if (cacheExpireSeconds > MAX_EXPIRE) {
            throw new RuntimeException(String.format(
                    "The salesforce.cache-expire-seconds of %d is set too high.  Maximum is %d seconds.",
                    cacheExpireSeconds, MAX_EXPIRE));
        }
        this.cacheExpireSeconds = cacheExpireSeconds;
        return this;
    }
}
