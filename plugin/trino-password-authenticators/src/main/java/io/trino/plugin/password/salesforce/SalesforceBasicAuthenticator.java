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
package io.trino.plugin.password.salesforce;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.escape.Escaper;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.log.Logger;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.plugin.password.Credential;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.PasswordAuthenticator;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.security.Principal;
import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.xml.XmlEscapers.xmlContentEscaper;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    // Set of Salesforce orgs, which users must belong to in order to authN.
    private final Set<String> allowedOrganizations;
    private final HttpClient httpClient;
    private final NonEvictableLoadingCache<Credential, Principal> userCache;

    @Inject
    public SalesforceBasicAuthenticator(SalesforceConfig config, @SalesforceAuthenticationClient HttpClient httpClient)
    {
        this.allowedOrganizations = ImmutableSet.copyOf(config.getOrgSet());
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.userCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .maximumSize(config.getCacheSize())
                        .expireAfterWrite(config.getCacheExpireDuration().toMillis(), MILLISECONDS),
                CacheLoader.from(this::doLogin));
    }

    @Override
    public Principal createAuthenticatedPrincipal(String username, String password)
    {
        try {
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
        log.debug("Logging into Salesforce.");
        String username = credential.getUser();
        String password = credential.getPassword();

        // Login requests must be POSTs
        String loginSoapMessage = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n" +
                "<env:Envelope xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"\n" +
                "xmlns:urn=\"urn:enterprise.soap.sforce.com\"\n" +
                "   xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                "   xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\">\n" +
                " <env:Header>\n" +
                "     <urn:CallOptions>\n" +
                "       <urn:client>presto</urn:client>\n" +
                "     </urn:CallOptions>\n" +
                " </env:Header>\n" +
                " <env:Body>\n" +
                "   <n1:login xmlns:n1=\"urn:partner.soap.sforce.com\">\n" +
                "     <n1:username>%s</n1:username>\n" +
                "     <n1:password>%s</n1:password>\n" +
                "   </n1:login>\n" +
                " </env:Body>\n" +
                "</env:Envelope>\n";
        String apiVersion = "46.0";
        String loginUrl = "https://login.salesforce.com/services/Soap/u/";
        Escaper escaper = xmlContentEscaper();
        Request request = new Request.Builder()
                .setUri(URI.create(loginUrl + apiVersion))
                .setHeader("Content-Type", "text/xml;charset=UTF-8")
                .setHeader("SOAPAction", "login")
                .setMethod("POST")
                .setBodyGenerator(createStaticBodyGenerator(format(loginSoapMessage, escaper.escape(username), escaper.escape(password)), UTF_8))
                .build();

        StringResponseHandler.StringResponse response = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());

        if (response.getStatusCode() != 200) {
            throw new AccessDeniedException(format("Invalid response for login\n.%s",
                    response.getBody()));
        }

        Document xmlResponse;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            xmlResponse = builder.parse(new InputSource(new StringReader(
                    response.getBody())));
        }
        catch (ParserConfigurationException | SAXException | IOException e) {
            throw new RuntimeException(format("Error parsing response: %s\n\tReceived error message: %s",
                    response.getBody(),
                    e.getMessage()));
        }

        // Make sure a Session Id has been returned.
        getElementValue(xmlResponse, "sessionId");

        // We want the organizationId from the response to compare it to the configured org from password-authenticator.properties.
        String returnedOrg = getElementValue(xmlResponse, "organizationId");
        // If the only entry in the set is "all", don't bother to check, otherwise make sure the returned org is in the set.
        // The organizationId is always in Locale.US, regardless of the user's locale and language.
        if (!allowedOrganizations.equals(ImmutableSet.of("all"))) {
            if (!allowedOrganizations.contains(returnedOrg.toLowerCase(Locale.US))) {
                throw new AccessDeniedException(format(
                        "Login successful, but for wrong Salesforce org.  Got %s, but expected a different org.",
                        returnedOrg));
            }
        }
        return new BasicPrincipal(username);
    }

    // Finds the text value of an element, which must appear only once in an XML document.
    // We will use this to find the organizationId and approve the authenticated user.
    private static String getElementValue(Document document, String elementName)
    {
        NodeList nodeList = document.getElementsByTagName(elementName);
        if (nodeList.getLength() == 0) {
            throw new RuntimeException(format("Salesforce login response does not contain a '%s' entry", elementName));
        }
        if (nodeList.getLength() > 1) {
            throw new RuntimeException(format("Salesforce login response contains multiple '%s' entries", elementName));
        }
        String content = emptyToNull(nodeList.item(0).getTextContent());
        if (content == null) {
            throw new RuntimeException(format("Salesforce login response contains an empty '%s' entry", elementName));
        }
        return content;
    }
}
