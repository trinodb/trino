Data generated using OSS Delta Lake 3.3.0:

```sql
CREATE TABLE clone_merge_deletion_vector_source (
    id int, v string, part date)    
    USING DELTA    
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
    
INSERT INTO clone_merge_deletion_vector_source VALUES     
    (1, 'A', TIMESTAMP '2024-01-01'),     
    (2, 'B', TIMESTAMP '2024-01-01'),     
    (3, 'C', TIMESTAMP '2024-02-02'),     
    (4, 'D', TIMESTAMP '2024-02-02'),     
    (5, 'E', TIMESTAMP '2025-01-01'),     
    (6, 'F', TIMESTAMP '2025-01-01');

OPTIMIZE clone_merge_deletion_vector_source;
    
DELETE FROM clone_merge_deletion_vector_source WHERE id IN (5, 6);
```
