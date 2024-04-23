README.txt

This folder contains code for creating MongoDB documents and views for use by Trino.

Subfolders are by MongoDB database, like:
- <database>
  - schemas
    - (Mongosh create scripts for Trino schema documents for collections and views in <database>)
  - views
    - (Mongosh create scripts for MongoDB views in <database>)

Some of the create scripts for Trino schema documents may be templates and need to have text replaced.
These scripts will have one or more of the following strings in their file names:
  - TENANT_UUID
  - INSTANCE_UUID
