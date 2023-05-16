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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class LdapUtil
{
    public static final String MEMBER = "member";
    public static final String MEMBEROF = "memberOf";

    private LdapUtil()
    {}

    @SuppressWarnings("BanJNDI")
    public static String addLdapDefinition(LdapObjectDefinition ldapObjectDefinition, DirContext context)
    {
        requireNonNull(ldapObjectDefinition, "LDAP Object Definition is null");
        Attribute objectClass = new BasicAttribute("objectClass");
        ldapObjectDefinition.getObjectClasses().forEach(objectClass::add);
        final Attributes entries = new BasicAttributes();
        entries.put(objectClass);
        // we must deal with multivalued attribute keys, can't use ::add as for objectClass
        final Collection<Map.Entry<String, String>> attrs = ldapObjectDefinition.getAttributes().entries();
        final HashMap<String, Attribute> hashmap = new HashMap<>();
        attrs.stream().forEach(a -> {
            if (hashmap.containsKey(a.getKey())) {
                hashmap.get(a.getKey()).add(a.getValue());
            }
            else {
                hashmap.put(a.getKey(), new BasicAttribute(a.getKey(), a.getValue()));
            }
        });
        hashmap.forEach((k, v) -> {
            entries.put(v);
        });

        try {
            context.createSubcontext(ldapObjectDefinition.getDistinguishedName(), entries);
        }
        catch (NamingException e) {
            throw new RuntimeException("LDAP Entry addition failed", e);
        }

        return ldapObjectDefinition.getDistinguishedName();
    }

    @SuppressWarnings("BanJNDI")
    public static void addAttributesToExistingLdapObjects(String distinguishedName,
            Map<String, List<String>> modifiedAttributes,
            DirContext context)
    {
        requireNonNull(distinguishedName, "distinguishedName is null");
        requireNonNull(modifiedAttributes, "modifiedAttributes is null");

        ModificationItem[] modificationItems = modifiedAttributes.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(
                        attribute -> new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute(entry.getKey(), attribute))))
                .toArray(ModificationItem[]::new);

        try {
            context.modifyAttributes(distinguishedName, modificationItems);
        }
        catch (NamingException e) {
            throw new RuntimeException("LDAP Entry update failed", e);
        }
    }

    public static LdapObjectDefinition buildLdapOrganizationObject(String name, String baseDistinguisedName)
    {
        return LdapObjectDefinition.builder(name)
                .setDistinguishedName(format("ou=%s,%s", name, baseDistinguisedName))
                .setAttributes(ImmutableMultimap.of("ou", name))
                .setObjectClasses(ImmutableList.of("top", "organizationalUnit"))
                .build();
    }

    public static LdapObjectDefinition buildLdapGroupObject(String organizationName, String groupName)
    {
        return LdapObjectDefinition.builder(groupName)
                .setDistinguishedName(format("cn=%s,%s", groupName, organizationName))
                .setAttributes(ImmutableMultimap
                        .of("cn", groupName, MEMBER, format("uid=default-%s,%s", groupName, organizationName)))
                .setObjectClasses(ImmutableList.of("groupOfNames"))
                .build();
    }

    public static LdapObjectDefinition buildLdapGroupObject(String organizationName, String groupName, String groupMembershipAttribute, String userDN)
    {
        return LdapObjectDefinition.builder(groupName)
                .setDistinguishedName(format("cn=%s,%s", groupName, organizationName))
                .setAttributes(ImmutableMultimap
                        .of("cn", groupName, groupMembershipAttribute, userDN))
                .setObjectClasses(ImmutableList.of("groupOfNames"))
                .build();
    }

    public static LdapObjectDefinition buildLdapUserObject(String organizationName, String userName, String password)
    {
        return LdapObjectDefinition.builder(userName)
                .setDistinguishedName(format("uid=%s,%s", userName, organizationName))
                .setAttributes(ImmutableMultimap.of("cn", userName, "sn", userName, "userPassword", password))
                .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                .build();
    }

    public static LdapObjectDefinition buildLdapUserObject(String organizationName, String userName, String password, Set<String> groupNames)
    {
        Multimap<String, String> memberOf = ArrayListMultimap.create();
        groupNames.forEach(g -> memberOf.put(MEMBEROF, g));
        ImmutableListMultimap<String, String> attrs = new ImmutableListMultimap.Builder<String, String>()
                .put("cn", userName)
                .put("sn", userName)
                .put("userPassword", password)
                .put("instanceType", "0")
                .put("nTSecurityDescriptor", "00")
                .put("objectCategory", "cn=user" + userName)
                .build();
        ImmutableListMultimap<String, String> attributes = new ImmutableListMultimap.Builder<String, String>()
                .putAll(attrs)
                .putAll(memberOf)
                .build();
        return LdapObjectDefinition.builder(userName)
                .setDistinguishedName(format("uid=%s,%s", userName, organizationName))
                .setAttributes(attributes)
                .setObjectClasses(Arrays.asList("person", "user"))
                .build();
    }
}
