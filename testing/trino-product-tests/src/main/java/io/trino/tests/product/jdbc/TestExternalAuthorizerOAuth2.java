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
package io.trino.tests.product.jdbc;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.jdbc.TestingRedirectHandlerInjector;
import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.TpchTableResults;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryResult.forResultSet;
import static io.trino.tests.product.TestGroups.OAUTH2;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openqa.selenium.support.ui.ExpectedConditions.elementToBeClickable;

public class TestExternalAuthorizerOAuth2
        extends ProductTest
{
    private static final Logger log = Logger.get(TestExternalAuthorizerOAuth2.class);

    @Inject
    @Named("databases.presto.jdbc_url")
    String jdbcUrl;

    private final ExecutorService executor = newSingleThreadExecutor();
    private Future<?> lastLoginAction;

    @AfterClass(alwaysRun = true)
    public void clean()
            throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination(10, SECONDS);
    }

    @Test(groups = {OAUTH2, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = "https://github.com/trinodb/trino/issues/6991", match = "Last login action has failed with exception")
    public void shouldAuthenticateAndExecuteQuery()
            throws Exception
    {
        prepareSeleniumHandler();
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            assertSuccessfulLogin();
            assertThat(forResultSet(results)).matches(TpchTableResults.PRESTO_NATION_RESULT);
        }
    }

    @Test(groups = {OAUTH2, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = "https://github.com/trinodb/trino/issues/6991", match = "Last login action has failed with exception")
    public void shouldAuthenticateAfterTokenExpires()
            throws Exception
    {
        prepareSeleniumHandler();
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            assertSuccessfulLogin();
            //Wait until the token expires. See: HydraIdentityProvider.TTL_ACCESS_TOKEN_IN_SECONDS
            SECONDS.sleep(10);

            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                assertSuccessfulLogin();
                assertThat(forResultSet(repeatedResults)).matches(TpchTableResults.PRESTO_NATION_RESULT);
            }
        }
    }

    private void prepareSeleniumHandler()
    {
        lastLoginAction = immediateFailedFuture(new AssertionError("Login action has not been triggered"));
        TestingRedirectHandlerInjector.setRedirectHandler(uri -> {
            lastLoginAction = executor.submit(() -> {
                WebDriver driver = getWebDriver();
                driver.get(uri.toString());
                WebDriverWait wait = new WebDriverWait(driver, 10);
                submitCredentials(driver, "foo@bar.com", "foobar", wait);
                giveConsent(driver, wait);
            });
        });
    }

    private WebDriver getWebDriver()
    {
        ChromeOptions options = new ChromeOptions();
        options.setAcceptInsecureCerts(true);
        return new RemoteWebDriver(getWebDriverUrl(), options);
    }

    private URL getWebDriverUrl()
    {
        try {
            return new URL("http", "selenium-chrome", 7777, "/wd/hub");
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private void submitCredentials(WebDriver driver, String email, String password, WebDriverWait wait)
    {
        log.info("trying to submit credentials");
        By emailElementLocator = By.id("email");
        log.info("waiting for email field");
        wait.until(elementToBeClickable(emailElementLocator));
        log.info("email field found");
        WebElement usernameElement = driver.findElement(emailElementLocator);
        usernameElement.sendKeys(email);
        log.info("email field set to %s", email);
        log.info("waiting for password field");
        By passwordElementLocator = By.id("password");
        wait.until(elementToBeClickable(passwordElementLocator));
        log.info("password field found");
        WebElement passwordElement = driver.findElement(passwordElementLocator);
        passwordElement.sendKeys(password + "\n");
        log.info("password field set to %s", password);
    }

    private void giveConsent(WebDriver driver, WebDriverWait wait)
    {
        log.info("trying to give consent");
        log.info("waiting for openId checkbox");
        By openIdCheckboxLocator = By.id("openid");
        wait.until(elementToBeClickable(openIdCheckboxLocator));
        log.info("openId checkbox found");
        WebElement openIdCheckbox = driver.findElement(openIdCheckboxLocator);
        openIdCheckbox.click();
        log.info("openId checkbox clicked");
        log.info("waiting for accept button");
        By acceptButtonLocator = By.id("accept");
        wait.until(elementToBeClickable(acceptButtonLocator));
        log.info("accept button found");
        WebElement acceptButton = driver.findElement(acceptButtonLocator);
        acceptButton.click();
        log.info("accept button clicked");
    }

    private void assertSuccessfulLogin()
            throws Exception
    {
        try {
            lastLoginAction.get();
        }
        catch (ExecutionException e) {
            if (e.getCause() != null) {
                throw new AssertionError("Last login action has failed with exception", e.getCause());
            }
            throw e;
        }
    }
}
