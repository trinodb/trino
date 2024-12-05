# Azure Storage file system support

Trino includes a native implementation to access [Azure
Storage](https://learn.microsoft.com/en-us/azure/storage/) with a catalog using
the Delta Lake, Hive, Hudi, or Iceberg connectors.

Enable the native implementation with `fs.native-azure.enabled=true` in your
catalog properties file.

## General configuration

Use the following properties to configure general aspects of Azure Storage file
system support:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `fs.native-azure.enabled`
  - Activate the native implementation for Azure Storage support. Defaults to
    `false`. Set to `true` to use Azure Storage and enable all other properties.
* - `azure.auth-type`
  - Authentication type to use for Azure Storage access. Defaults no
    authentication used with `NONE`. Use `ACCESS_KEY` for
    [](azure-access-key-authentication) or and `OAUTH` for
    [](azure-oauth-authentication).
* - `azure.endpoint`
  - Hostname suffix of the Azure storage endpoint.
    Defaults to `core.windows.net` for the global Azure cloud.
    Use `core.usgovcloudapi.net` for the Azure US Government cloud,
    `core.cloudapi.de` for the Azure Germany cloud,
    or `core.chinacloudapi.cn` for the Azure China cloud.
* - `azure.read-block-size`
  - [Data size](prop-type-data-size) for blocks during read operations. Defaults
    to `4MB`.
* - `azure.write-block-size`
  - [Data size](prop-type-data-size) for blocks during write operations.
    Defaults to `4MB`.
* - `azure.max-write-concurrency`
  - Maximum number of concurrent write operations. Defaults to 8.
* - `azure.max-single-upload-size`
  - [Data size](prop-type-data-size) Defaults to `4MB`.
* - `azure.max-http-requests`
  - Maximum [integer](prop-type-integer) number of concurrent HTTP requests to
    Azure from every node. Defaults to double the number of processors on the
    node. Minimum `1`. Use this property to reduce the number of requests when
    you encounter rate limiting issues.
* - `azure.application-id`
  - Specify the application identifier appended to the `User-Agent` header
    for all requests sent to Azure Storage. Defaults to `Trino`. 
:::

(azure-access-key-authentication)=
## Access key authentication

Use the following properties to configure access key authentication to Azure
Storage:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `azure.auth-type`
  - Must be set to `ACCESS_KEY`.
* - `azure.access-key`
  - The decrypted access key for the Azure Storage account. Requires
    authentication type `ACCESSS_KEY`.
:::

(azure-oauth-authentication)=
## OAuth 2.0 authentication

Use the following properties to configure OAuth 2.0 authentication to Azure
Storage:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `azure.auth-type`
  - Must be set to `OAUTH`.
* - `azure.oauth.tenant-id`
  - Tenant ID for Azure authentication.
* - `azure.oauth.endpoint`
  - The endpoint URL for OAuth 2.0 authentication.
* - `azure.oauth.client-id`
  - The OAuth 2.0 service principal's client or application ID.
* - `azure.oauth.secret`
  - A OAuth 2.0 client secret for the service principal.
:::

## Access multiple storage accounts

To allow Trino to access multiple Azure storage accounts from a single
catalog configuration, you can use [](azure-oauth-authentication) with
an Azure service principal. The following steps describe how to create
a service principal in Azure and assign an IAM role granting access to the
storage accounts:

- Create a service principal in Azure Active Directory using Azure
  **App Registrations** and save the client secret.
- Assign access to the storage accounts from the account's
  **Access Control (IAM)** section. You can add **Role Assignments** and
  select appropriate roles, such as **Storage Blob Data Contributor**.
- Assign access using the option **User, group, or service principal** and
  select the service principal created. Save to finalize the role
  assignment.

 Once you create the service principal and configure the storage accounts
 use the **Client ID**, **Secret** and **Tenant ID** values from the
 application registration, to configure the catalog using properties from
 [](azure-oauth-authentication).
