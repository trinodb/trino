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
package io.trino.tests.product;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.tempto.Requirement;
import io.trino.tempto.fulfillment.ldap.LdapObjectDefinition;
import io.trino.tempto.fulfillment.ldap.LdapObjectRequirement;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public final class ImmutableLdapObjectDefinitions
{
    private static final String DOMAIN = "dc=trino,dc=testldap,dc=com";
    private static final String AMERICA_DISTINGUISHED_NAME = format("ou=America,%s", DOMAIN);
    private static final String ASIA_DISTINGUISHED_NAME = format("ou=Asia,%s", DOMAIN);
    private static final String EUROPE_DISTINGUISHED_NAME = format("ou=Europe,%s", DOMAIN);
    private static final String LDAP_PASSWORD = "LDAPPass123";
    private static final String MEMBER = "member";

    private ImmutableLdapObjectDefinitions()
    {}

    public static final LdapObjectDefinition AMERICA_ORG = buildLdapOrganizationObject("America", AMERICA_DISTINGUISHED_NAME, "America");

    public static final LdapObjectDefinition ASIA_ORG = buildLdapOrganizationObject("Asia", ASIA_DISTINGUISHED_NAME, "Asia");

    public static final LdapObjectDefinition EUROPE_ORG = buildLdapOrganizationObject("Europe", EUROPE_DISTINGUISHED_NAME, "Europe");

    public static final LdapObjectDefinition DEFAULT_GROUP_USER = buildLdapUserObject("DefaultGroupUser", LDAP_PASSWORD);

    public static final LdapObjectDefinition PARENT_GROUP_USER = buildLdapUserObject("ParentGroupUser", LDAP_PASSWORD);

    public static final LdapObjectDefinition CHILD_GROUP_USER = buildLdapUserObject("ChildGroupUser", LDAP_PASSWORD);

    public static final LdapObjectDefinition ORPHAN_USER = buildLdapUserObject("OrphanUser", LDAP_PASSWORD);

    public static final LdapObjectDefinition SPECIAL_USER = buildLdapUserObject("User WithSpecialPwd", "LDAP:Pass ~!@#$%^&*()_+{}|:\"<>?/.,';\\][=-`");

    public static final LdapObjectDefinition USER_IN_MULTIPLE_GROUPS = buildLdapUserObject("UserInMultipleGroups", LDAP_PASSWORD);

    public static final LdapObjectDefinition USER_IN_EUROPE = buildLdapUserObject("EuropeUser", EUROPE_DISTINGUISHED_NAME, LDAP_PASSWORD);

    public static final LdapObjectDefinition USER_IN_AMERICA = buildLdapUserObject("AmericanUser", AMERICA_DISTINGUISHED_NAME, LDAP_PASSWORD);

    public static final LdapObjectDefinition CHILD_GROUP = buildLdapGroupObject("ChildGroup", "ChildGroupUser", ImmutableList.of());

    public static final LdapObjectDefinition DEFAULT_GROUP = buildLdapGroupObject(
            "DefaultGroup",
            "DefaultGroupUser",
            ImmutableList.of(CHILD_GROUP, SPECIAL_USER, USER_IN_MULTIPLE_GROUPS, USER_IN_EUROPE, USER_IN_AMERICA));

    public static final LdapObjectDefinition PARENT_GROUP = buildLdapGroupObject(
            "ParentGroup",
            "ParentGroupUser",
            ImmutableList.of(DEFAULT_GROUP, USER_IN_MULTIPLE_GROUPS));

    public static Requirement getLdapRequirement()
    {
        return new LdapObjectRequirement(
                ImmutableList.of(
                        AMERICA_ORG, ASIA_ORG, EUROPE_ORG,
                        DEFAULT_GROUP_USER, PARENT_GROUP_USER, CHILD_GROUP_USER, ORPHAN_USER, SPECIAL_USER, USER_IN_MULTIPLE_GROUPS, USER_IN_AMERICA, USER_IN_EUROPE,
                        DEFAULT_GROUP, PARENT_GROUP, CHILD_GROUP));
    }

    public static LdapObjectDefinition buildLdapOrganizationObject(String id, String distinguishedName, String unit)
    {
        return LdapObjectDefinition.builder(id)
                .setDistinguishedName(distinguishedName)
                .setAttributes(ImmutableMap.of("ou", unit))
                .setObjectClasses(Arrays.asList("top", "organizationalUnit"))
                .build();
    }

    public static LdapObjectDefinition buildLdapGroupObject(String groupName, String userName, List<LdapObjectDefinition> memberAttributes)
    {
        return buildLdapGroupObject(groupName, AMERICA_DISTINGUISHED_NAME, userName, ASIA_DISTINGUISHED_NAME, memberAttributes);
    }

    public static LdapObjectDefinition buildLdapGroupObject(
            String groupName,
            String groupOrganizationName,
            String userName,
            String userOrganizationName,
            List<LdapObjectDefinition> memberAttributes)
    {
        return LdapObjectDefinition.builder(groupName)
                .setDistinguishedName(format("cn=%s,%s", groupName, groupOrganizationName))
                .setAttributes(ImmutableMap.of(
                        "cn", groupName,
                        "member", format("uid=%s,%s", userName, userOrganizationName)))
                .setModificationAttributes(ImmutableMap.of(
                        MEMBER,
                        memberAttributes.stream()
                                .map(LdapObjectDefinition::getDistinguishedName)
                                .collect(toImmutableList())))
                .setObjectClasses(Arrays.asList("groupOfNames"))
                .build();
    }

    public static LdapObjectDefinition buildLdapUserObject(String userName, String password)
    {
        return buildLdapUserObject(userName, ASIA_DISTINGUISHED_NAME, password);
    }

    public static LdapObjectDefinition buildLdapUserObject(String userName, String userOrganizationName, String password)
    {
        return LdapObjectDefinition.builder(userName)
                .setDistinguishedName(format("uid=%s,%s", userName, userOrganizationName))
                .setAttributes(ImmutableMap.of(
                        "cn", userName,
                        "sn", userName,
                        "userPassword", password))
                .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                .build();
    }
}
