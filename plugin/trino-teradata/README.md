# Teradata Connector Developer Notes

The Teradata connector module has both unit tests and integration tests.
The integration tests require access to a [Teradata ClearScape Analytics™ Experience](https://clearscape.teradata.com/sign-in).
You can follow the steps below to run the integration tests locally.

## Prerequisites

#### 1. Create a new ClearScape Analytics™ Experience account

If you don't already have one, sign up at:

[Teradata ClearScape Analytics™ Experience](https://www.teradata.com/getting-started/demos/clearscape-analytics)

#### 2. Login

Sign in with your new account at:

[ClearScape Analytics™ Experience Login](https://clearscape.teradata.com/sign-in)

#### 3. Collect the API Token

Use the **Copy API Token** button in the UI to retrieve your token.

#### 4. Define the following environment variables

⚠️ **Note:** The Teradata database password must be **at least 8 characters long**.

```
export CLEARSCAPE_TOKEN=<API Token>
export CLEARSCAPE_PASSWORD=<Password for Teradata database (min 8 chars)>
```

## Running Integration Tests

Once the environment variables are set, run the integration tests with:

⚠️ **Note:** Run the following command from the Trino parent directory.

```
 ./mvnw clean install -pl :trino-teradata
```
