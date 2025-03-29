# Description of resource files

## schema_registry_jaas_config.conf
Standard JAAS Login Configuration File
This implementation will:
* enable PropertyFileLoginModule
* set password file to `/etc/confluent/docker/password-file` (this will be copied from [schema_registry_password-file](./schema_registry_password-file))
* enable debug logs for more visibility 

Reference:  
https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html

## schema_registry_password-file
Confluent password file.  
Contains usernames, password hashes and roles.  
File format:
```
<username>: <password-hash>,<role1>[,<role2>,...]
```

Reference:  
https://docs.confluent.io/platform/current/schema-registry/security/index.html#configuring-the-rest-api-for-basic-http-authentication
