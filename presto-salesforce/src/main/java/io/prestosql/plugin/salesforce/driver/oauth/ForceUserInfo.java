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
package io.prestosql.plugin.salesforce.driver.oauth;

import com.google.api.client.util.Key;

import java.util.Map;

public class ForceUserInfo
{
    @Key("user_id") private String userId;
    @Key("organization_id") private String organizationId;
    @Key("preferred_username") private String preferredUsername;
    @Key("nickname") private String nickName;
    private String name;
    private String email;
    @Key("zoneinfo") private String timeZone;
    @Key("locale") private String locale;
    private String instance;
    private String partnerUrl;
    @Key("urls") private Map<String, String> urls;

    public String getUserId()
    {
        return userId;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    public String getOrganizationId()
    {
        return organizationId;
    }

    public void setOrganizationId(String organizationId)
    {
        this.organizationId = organizationId;
    }

    public String getPreferredUsername()
    {
        return preferredUsername;
    }

    public void setPreferredUsername(String preferredUsername)
    {
        this.preferredUsername = preferredUsername;
    }

    public String getNickName()
    {
        return nickName;
    }

    public void setNickName(String nickName)
    {
        this.nickName = nickName;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }

    public String getTimeZone()
    {
        return timeZone;
    }

    public void setTimeZone(String timeZone)
    {
        this.timeZone = timeZone;
    }

    public String getLocale()
    {
        return locale;
    }

    public void setLocale(String locale)
    {
        this.locale = locale;
    }

    public String getInstance()
    {
        return instance;
    }

    public void setInstance(String instance)
    {
        this.instance = instance;
    }

    public String getPartnerUrl()
    {
        return partnerUrl;
    }

    public void setPartnerUrl(String partnerUrl)
    {
        this.partnerUrl = partnerUrl;
    }

    public Map<String, String> getUrls()
    {
        return urls;
    }

    public void setUrls(Map<String, String> urls)
    {
        this.urls = urls;
    }
}
