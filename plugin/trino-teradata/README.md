# Teradata Connector Developer Notes
The Teradata connector module has both unit tests and integration tests. 
The integration tests require access to a Teradata ClearScape environment.
You can follow the steps below to be able to run the integration tests locally.


## Requirements
* Access to a Teradata ClearScape account. You can create one by visiting the [Teradata ClearScape portal](https://clearscape.teradata.com/sign-in).
* Valid Teradata ClearScape API credentials (token and environment name).

## Steps
* Enable ClearScape access in your Teradata account.
* Build the project by following the instructions in the main documentation.
* Obtain ClearScape credentials:
    * Log into the ClearScape portal
    * Copy the API token of your account
    * Note your environment name or create a new environment
* Configure test properties: Set the following VM options in your IntelliJ "Run Configuration":
    * teradata.clearscape.token=your-api-token
    * teradata.clearscape.password=your-environment-password
* Environment validation:
    * The connector automatically validates that the ClearScape URL points to [api.clearscape.teradata.com](https://api.clearscape.teradata.com/api-docs/?_gl=1*z35r6m*_gcl_au*MTk4NDg2NzY5NC4xNzUwOTQwNjMx*_ga*MTE3NTQ5MzU3OS4xNzUwNDk4Nzk3*_ga_7PE2TMW3FE*czE3NTI4MTkzMjEkbzE4JGcwJHQxNzUyODE5MzY5JGoxMiRsMCRoMA..)
    * Only HTTPS/HTTP URLs to the official Teradata ClearScape domain are allowed
* Test data setup:
    * The integration tests will automatically provision and start a ClearScape environment
    * Test data will be created as part of the test execution
    * Environment cleanup happens automatically after tests complete
* Run the tests:
    * Execute any integration test class
    * The ClearScapeManager will handle environment lifecycle (create, start, stop, teardown)
    * Tests will connect to the provisioned Teradata instance via JDBC
