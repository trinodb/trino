Data generated using OSS Delta Lake 3.3.0:

```sql
CREATE TABLE clone_merge_deletion_vector_cloned SHALLOW CLONE clone_merge_deletion_vector_source;

ALTER TABLE clone_merge_deletion_vector_cloned SET TBLPROPERTIES (delta.enableDeletionVectors = true);
```
