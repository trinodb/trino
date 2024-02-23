# Trino Filesystem Implementation for Azure

## Azure CI/CD Infrastructure setup

In the Azure subscription "Pay-As-You-Go(active)" with the 

`Subscription ID: 875ba59f-b2f5-462d-b161-38f1e865364f`

there has been created the resource group:

`starburst_dev_cicd`

### OAuth2 Infrastructure setup

In the `starburst_dev_cicd` resource group there has been created the app registration

`starburst-dev-cicd`

It has the following properties:

- Application (client) ID: `c0715412-0d4d-4253-b0e9-b44afec55df3`
- Directory (tenant) ID: `9ac50357-7ce0-4d4f-83d3-d8a10c328c05`

Another important characteristic of this app registration can be  found
in the _Endpoint_ section

OAuth 2.0 token endpoint (v2)
https://login.microsoftonline.com/9ac50357-7ce0-4d4f-83d3-d8a10c328c05/oauth2/v2.0/token

For testing OAuth2.0 related scenarios, the app registration needs as well
a secret. Use the section _Certificates & secrets_ to setup a new secret.
A software engineer may need permissions to perform such an action (at the time of this
writing, the necessary permissions have been requested through _Entitle_ application).


### Storage accounts

For testing the integration with Azure, there have been created two storage accounts:

- `devcicdflat` : has the _Hierarchical namespace_ property disabled
- `devcicdhierarchical` : has the _Hierarchical namespace_ property enabled

When the _Hiearchical namespace_ is enabled, this allows the collection of objects/files within
a storage account to be organized into a hierarchy of directories and nested subdirectories in 
the same way that the file system on your computer is organized.

In the above mentioned storage accounts, configure in the section

_Access Control (IAM)_ > _Roles_

the assignments to the app registration `starburst-dev-cicd` for the following roles:

- `Contributor`
- `Storage Blob Data Owner`

This is done to provide the necessary permissions for OAuth2.0 related scenarios.

The access key for accessing the storage account for generic scenarios (not OAuth2.0 scenarios)
can be found in the _Access keys_ section.
