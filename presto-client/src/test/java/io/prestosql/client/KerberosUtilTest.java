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
package io.prestosql.client;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class KerberosUtilTest
{
    private static final Attributes ALIAS_ONE = new BasicAttributes();
    private static final Attributes ALIAS_TWO = new BasicAttributes();
    private static final Attributes SERVICE_NAME = new BasicAttributes();
    private static final Attributes A_RECORD = new BasicAttributes();

    private static final String ALIAS_ONE_VALUE = "example2.com";
    private static final String ALIAS_TWO_VALUE = "example3.com";
    private static final String SERVICE_NAME_VALUE = "proxy.com";
    private static final String A_RECORD_VAL = "10.20.54.123";
    private static final String HOST = "example.com";
    private static final String HOST_NOCHAIN = "nochain.com";
    private static final String HOST_NOMAPPING = "nomapping.com";
    private static DirContext ictx;

    @BeforeTest
    public void setUp() throws NamingException
    {
        ALIAS_ONE.put("CNAME", ALIAS_ONE_VALUE);
        ALIAS_TWO.put("CNAME", ALIAS_TWO_VALUE);
        SERVICE_NAME.put("CNAME", SERVICE_NAME_VALUE);
        A_RECORD.put("A", A_RECORD_VAL);

        ictx = mock(DirContext.class);

        when(ictx.getAttributes(HOST, new String[]{"CNAME", "A"})).thenReturn(ALIAS_ONE);
        when(ictx.getAttributes(ALIAS_ONE_VALUE, new String[]{"CNAME", "A"})).thenReturn(ALIAS_TWO);
        when(ictx.getAttributes(ALIAS_TWO_VALUE, new String[]{"CNAME", "A"})).thenReturn(SERVICE_NAME);
        when(ictx.getAttributes(SERVICE_NAME_VALUE, new String[]{"CNAME", "A"})).thenReturn(A_RECORD);
        when(ictx.getAttributes(HOST_NOCHAIN, new String[]{"CNAME", "A"})).thenReturn(A_RECORD);
        when(ictx.getAttributes(HOST_NOMAPPING, new String[]{"CNAME", "A"})).thenReturn(new BasicAttributes());
    }

    @Test
    public void testResolveMultipleAlias()
    {
        assertEquals(SERVICE_NAME_VALUE, KerberosUtil.resolve(HOST, ictx));
    }

    @Test
    public void testResolveNoAlias()
    {
        assertEquals(HOST_NOCHAIN, KerberosUtil.resolve(HOST_NOCHAIN, ictx));
    }

    @Test
    public void testResolveClientException()
    {
        assertEquals(HOST_NOMAPPING, KerberosUtil.resolve(HOST_NOMAPPING, ictx));
    }
}
