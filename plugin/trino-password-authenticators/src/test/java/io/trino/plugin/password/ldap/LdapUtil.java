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
package io.trino.plugin.password.ldap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class LdapUtil
{
    private static final SecureRandom random = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 10;

    public static final String MEMBER = "member";

    private LdapUtil()
    {}

    public static String addLdapDefinition(LdapObjectDefinition ldapObjectDefinition, DirContext context)
    {
        requireNonNull(ldapObjectDefinition, "LDAP Object Definition is null");

        Attributes entries = new BasicAttributes();
        Attribute objectClass = new BasicAttribute("objectClass");

        ldapObjectDefinition.getAttributes()
                .forEach((key, value) -> entries.put(new BasicAttribute(key, value)));

        ldapObjectDefinition.getObjectClasses()
                .forEach(objectClass::add);
        entries.put(objectClass);

        try {
            context.createSubcontext(ldapObjectDefinition.getDistinguishedName(), entries);
        }
        catch (NamingException e) {
            throw new RuntimeException("LDAP Entry addition failed", e);
        }

        return ldapObjectDefinition.getDistinguishedName();
    }

    public static void addAttributesToExistingLdapObjects(String distinguishedName, Map<String, List<String>> modifiedAttributes, DirContext context)
    {
        requireNonNull(distinguishedName, "distinguishedName is null");
        requireNonNull(modifiedAttributes, "modifiedAttributes is null");

        ModificationItem[] modificationItems = modifiedAttributes.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(attribute -> new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute(entry.getKey(), attribute))))
                .toArray(ModificationItem[]::new);

        try {
            context.modifyAttributes(distinguishedName, modificationItems);
        }
        catch (NamingException e) {
            throw new RuntimeException("LDAP Entry updation failed", e);
        }
    }

    public static LdapObjectDefinition buildLdapOrganizationObject(String name, String baseDistinguisedName)
    {
        return LdapObjectDefinition.builder(name)
                .setDistinguishedName(format("ou=%s,%s", name, baseDistinguisedName))
                .setAttributes(ImmutableMap.of("ou", name))
                .setObjectClasses(ImmutableList.of("top", "organizationalUnit"))
                .build();
    }

    public static LdapObjectDefinition buildLdapGroupObject(String organizationName, String groupName)
    {
        return LdapObjectDefinition.builder(groupName)
                .setDistinguishedName(format("cn=%s,%s", groupName, organizationName))
                .setAttributes(ImmutableMap.of(
                        "cn", groupName,
                        MEMBER, format("uid=default-%s,%s", groupName, organizationName)))
                .setObjectClasses(ImmutableList.of("groupOfNames"))
                .build();
    }

    public static LdapObjectDefinition buildLdapUserObject(String organizationName, String userName, String password)
    {
        return LdapObjectDefinition.builder(userName)
                .setDistinguishedName(format("uid=%s,%s", userName, organizationName))
                .setAttributes(ImmutableMap.of(
                        "cn", userName,
                        "sn", userName,
                        "userPassword", password))
                .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                .build();
    }

    public static String randomSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }
}
