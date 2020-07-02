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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.log.Logger;
import io.prestosql.plugin.password.Credential;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.PasswordAuthenticator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

import javax.inject.Inject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.StringReader;
import java.net.URI;
import java.security.Principal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.prestosql.plugin.password.salesforce.SalesforceConfig.APIVERSION;
import static io.prestosql.plugin.password.salesforce.SalesforceConfig.LOGINURL;
import static io.prestosql.plugin.password.salesforce.SalesforceConfig.LOGIN_SOAP_MESSAGE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Allows users to authenticate to Presto using their Salesforce username and password + security token concatenation.  You can learn about
 * the Salesforce security token at https://help.salesforce.com/articleView?id=user_security_token.htm.
 * <br><br>
 * If your password is <code>foo</code> and token is <code>bar</code>, then your Presto password will be <code>foobar</code>.
 * <br><br>
 * Admins can configure one or more Salesforce Organization Ids (18 char) via <code>salesforce.org</code> in <code>password-authenticator.properties</code>
 * to act as a single layer of authZ.  Essentially, if the user who logs in is not part of the configured org, their access will be denied.  Alternatively,
 * the admin can specify "all", rather than actual orgs.  This will allow any authenticated Salesforce user access to Presto.
 */
public class SalesforceBasicAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(SalesforceBasicAuthenticator.class);

    private Set<String> orgs;                                   // Set of Salesforce orgs, which users must belong to in order to authN.
    private Map<String, String> responseMap = new HashMap<>();  // A flattened map of xml elements from the Salesforce login response.
    private String key = "";                                    // Will hold the xml element tag, we can add to the map with its text attribute.
    private final HttpClient httpClient;                        // An http client for posting to the Salesforce login endpoint.
    private final LoadingCache<Credential, Principal> userCache;

    private final Locale locale = Locale.US;                    // Tested API request and response for user with Japanese locale and language preference,
    // and responses are English, and organization id is not in Japaneses characters (this is
    // also true of the organization id in the UI, even with other text showing in Japanese).

    @Inject
    public SalesforceBasicAuthenticator(SalesforceConfig config, @SalesforceAuthenticationClient HttpClient httpClient)
    {
        this.orgs = config.getOrgSet();
        this.httpClient = httpClient;

        this.userCache = CacheBuilder.newBuilder()
                .maximumSize(config.getCacheSize())
                .expireAfterWrite(config.getCacheExpireSeconds(), TimeUnit.SECONDS)
                .build(CacheLoader.from(this::doLogin));
    }

    @Override
    public Principal createAuthenticatedPrincipal(String username, String password)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return userCache.getUnchecked(new Credential(username, password));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), AccessDeniedException.class);
            throw e;
        }
    }

    // This does the work of logging into Salesforce.
    private Principal doLogin(Credential credential)
    {
        log.info("Logging into Salesforce.");
        String username = credential.getUser();
        String password = credential.getPassword();

        // Login requests must be POSTs
        Request request = new Request.Builder()
                .setUri(URI.create(LOGINURL + APIVERSION))
                .setHeader("Content-Type", "text/xml;charset=UTF-8")
                .setHeader("SOAPAction", "login")
                .setMethod("POST")
                .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(String.format(LOGIN_SOAP_MESSAGE, username, password), UTF_8))
                .build();

        StringResponseHandler.StringResponse response = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());

        final int statusCode = response.getStatusCode();
        if (statusCode < 200 || statusCode >= 300) {
            throw new AccessDeniedException(String.format("Invalid response for login [%s]: %s. %s",
                    statusCode,
                    response.getBody(),
                    response.getHeaders()));
        }

        Document xmlResponse;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            xmlResponse = builder.parse(new InputSource(new StringReader(
                    response.getBody())));
        }
        catch (Exception e) {
            throw new RuntimeException(String.format("Error parsing response: %s\n\tReceived error message: %s",
                    response.getBody(),
                    e.getMessage()));
        }

        // Parse the response into a flattened map.
        parse((Node) xmlResponse);
        String sessionId = responseMap.get("sessionId");
        if (sessionId == null || sessionId.length() == 0) {
            throw new RuntimeException("Can't find accessToken in Salesforce login response.");
        }

        // We want the organizationId from the response to compare it to the configured org from password-authenticator.properties.
        String returnedOrg = responseMap.get("organizationId");
        if (returnedOrg == null || returnedOrg.length() == 0) {
            throw new RuntimeException("Can't find organizationId in Salesforce login response.");
        }
        // If the only entry in the set is "all", don't bother to check, otherwise make sure the returned org is in the set.
        if (!(orgs.size() == 1 && orgs.contains("all")) && !orgs.contains(returnedOrg.toLowerCase(locale))) {
            throw new AccessDeniedException(String.format(
                    "Login successful, but for wrong Salesforce org.  Got %s, but expected a different org.",
                    returnedOrg));
        }
        return new BasicPrincipal(username);
    }

    // This simply finds elements in the xml response, and adds them and their text values to the flattened map
    // We will use this to find the organizationId and approve the authenticated user.
    private void parse(Node node)
    {
        switch (node.getNodeType()) {
            case Node.ELEMENT_NODE:
                key = ((Element) node).getTagName();
                break;
            case Node.TEXT_NODE:
                if (key.length() > 0) {
                    responseMap.put(key, ((Text) node).getData());
                }
                break;
            default:
                key = "";
        }
        NodeList list = node.getChildNodes();
        for (int i = 0; i < list.getLength(); i++) {
            parse(list.item(i));
        }
    }
}
