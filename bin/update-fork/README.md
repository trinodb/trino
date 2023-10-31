# Update Starburst Trino common fork

## Script behaviour

Script `update-fork` performs cherry-picking of commits from the [trinodb/trino](https://github.com/trinodb/trino) 
repository and attempts to apply them to [chisk](https://github.com/starburstdata/chisk) repository.

It is designed to be run manually.

You may choose to run the script manually to help with manual conflict resolution.
It allows you to:

- Specify the commit to start cherry-picking from.
- Limit the number of commits to apply.
- Leaving cherry-pick in progress for manual conflict resolution.

Script starts cherry-picking after the latest applied cherry-picked commit. 
Cherry-picks are recognized by searching for `(cherry-pickef from commit ...)` lines in the commit message.
Therefore, when running cherry-pick manually, remember to use `-x` option to include above line in the message.

See `--help` for available options.

## Manually resolving conflict

To resolve a conflict, you have the option of altering commit for conflict resolution or skipping
a particular commit(s).  
In any case, make sure the latest cherry-picked commit has a proper line in the commit message,
referencing the original commit (as created with `-x` option for git cherry-pick command).
This allows for the script to properly recognize the latest applied to cherry-pick and continue automatic
operation.

You need to add a remote called `upstream`, pointing to the Trino repository, for example:

```shell
git remote add upstreamhttps://github.com/trinodb/trino
git fetch upstream 
```
