# Redshift Cluster Cleanup Script

This script allows you to delete AWS Redshift clusters that contain a specific substring in their name and are older than the specified number of hours.

## Requirements

- **AWS CLI**: The AWS Command Line Interface must be installed and configured with appropriate credentials. [Installation guide](https://aws.amazon.com/cli/)
- **jq**: A lightweight command-line JSON processor. [Installation guide](https://stedolan.github.io/jq/download/)
- **Bash**: Version 4.0 or higher

## Usage Examples

### Basic usage (dry run)
```bash
./cleanup-redshift.sh --name-substring "test-cluster" --age-hours 24
```

### Actually delete clusters (with confirmation)
```bash
./cleanup-redshift.sh --name-substring "test-cluster" --age-hours 24 --delete
```

### Delete clusters automatically without confirmation (DANGEROUS!)
```bash
./cleanup-redshift.sh --name-substring "test-cluster" --age-hours 48 --delete --auto-confirm
```

### Specify region and batch limit
```bash
./cleanup-redshift.sh --name-substring "ci-" --age-hours 72 --region "us-west-2" --delete-batch-limit 5 --delete
```

## Parameters

- `--name-substring` (required): Substring that must be present in cluster name to qualify for deletion
- `--age-hours` (optional, default: 24): Minimum age in hours for clusters to qualify for deletion
- `--region` (optional, default: us-east-1): AWS region to operate in
- `--delete-batch-limit` (optional, default: 10): Maximum number of clusters to delete at once
- `--delete` (optional): Actually perform deletion (without this, it's a dry run)
- `--auto-confirm` (optional): Skip user confirmation prompt (DANGEROUS!)
- `--output-file` (optional): Output file to save cluster identifiers

## Safety Features

1. **Dry run by default**: The script won't delete anything unless `--delete` flag is provided
2. **Required substring**: Clusters must contain the specified substring to qualify
3. **Age requirement**: Clusters must be older than specified hours
4. **User confirmation**: Prompts for confirmation before deletion (unless `--auto-confirm`)
5. **Batch limits**: Limits number of clusters processed in single run

## Example Output

```
Cleaning Redshift clusters...
region: us-east-1
name-substring: test-
age-hours: 48

[2025-09-09 09:00:00] AWS API call redshift describe-clusters
[2025-09-09 09:00:01] Cluster test-cluster-old qualified for deletion (age: 72 hours)
[2025-09-09 09:00:01] Cluster test-another-cluster qualified for deletion (age: 96 hours)
[2025-09-09 09:00:01] Found 2 Redshift clusters eligible for deletion

List of Redshift clusters to be deleted:
test-cluster-old
test-another-cluster

You are about to delete 2 Redshift clusters shown above!
This action is IRREVERSIBLE and will permanently delete the clusters and their data!
Are you sure (enter strict 'yes' to proceed)? 
```
