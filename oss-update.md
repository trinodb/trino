# Maintaining cork (Common Starburst Trino Fork)

## The Fork

Trino clusters used by Starburst (SEP/Galaxy) are not based on OSS Trino distribution (which lives
in https://github.com/trinodb/trino).
Instead, we use code which lives in https://github.com/starburstdata/cork.

Overall we operate in following GitHub repositories

* `trino` - https://github.com/trinodb/trino
* `cork` - https://github.com/starburstdata/cork

For brevity, we will use just names below.

## Development in cork

Development in `cork` repo happens on `master` branch.
The `master` branch in `cork` should always be release-ready and production quality.

We are updating `cork` `master` branch from `trino` on open-source release boundaries.
On top of that `cork` may contain commits which are cherry picks from `trino` which are not
yet part of a `trino` release, and commits which are internal to Starburst and to be shared between
Galaxy and SEP.

When `cork` is based on an old OSS release (`OLD`) and a new OSS release (`NEW`) made in `trino` we cherry pick all the
commits from `trino` using the steps outlined below.

### prerequisites

1. Make sure you have GNU versions of sed and find installed and in PATH. The PATH is usually displayed at the end of brew install.

```shell
# for sed
brew install gnu-sed
# Modify the PATH to make this executable your default for 'sed'. E.g: 
export PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"
 
# for find
brew install findutils
# Modify the PATH to make this executable your default for 'find'. E.g: 
export PATH="/usr/local/opt/findutils/libexec/gnubin:$PATH"
```

### update your local repository

```shell
# Fetch recent release tags from OSS 
git fetch --jobs 8 --all --prune --tags

# Make sure the local master branch is up to date
git checkout master && git pull --ff-only
```

### prepare

For sake of shell snippets please configure following shell variables (use proper version numbers of course):

```shell
# e.g. 410
OLD=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout | sed 's/-.*//')
# e.g. 411
NEW=$[OLD + 1]

# verify they got set correctly and without any whitespace
echo OLD="[${OLD}] NEW=[${NEW}]"
```

### create the update branch and update PR's placeholder

Create `update/cork/trino-${NEW}` starting at `master` branch.
Create the update PR before doing actual code import work.
The PR will serve as a place to store Action items that may occur during the code import process.


```shell
git checkout -b "update/cork/trino-${NEW}" "origin/master" &&
git commit --allow-empty --only -m "Empty placeholder commit for update to ${NEW}" `# this will disappear when merging the PR` &&
git push origin "update/cork/trino-${NEW}" -u &&
open "https://github.com/starburstdata/cork/compare/update/cork/trino-${NEW}?expand=1&title=Update+to+Trino+${NEW}&body=$(
python3 -c 'import urllib.parse, sys; print(urllib.parse.quote(sys.stdin.read()))' <<EOF
## Update cork to ${NEW}

Action items:
- [ ] Update project version
- [ ] Squash fixups
- [ ] Check OSS release notes for breaking changes
EOF
)&labels=salesforce"
```

### rebase OSS commits onto Cork codebase

Rebase all incoming commits from OSS, except for the ones created by maven-release-plugin.
We will update the version manually.

```shell
# The initial cherry pick is just to add "(cherry picked from commit ...)" to the commit messages
git reset --hard "${OLD}" &&
git rev-list --reverse "${OLD}..${NEW}" --invert-grep --grep '^\[maven-release-plugin]' | git cherry-pick -x --stdin &&
# Do the actual rebase
git rebase --interactive --empty=drop "${OLD}" --onto master

# Run the rebase, resolving the code conflicts as necessary and using git rebase --continue to continue
```

### update project version

As the maven-release-plugin commits were skipped during previous step, we need to change the version ourselves

```shell
./mvnw versions:set -DnewVersion="${NEW}-cork-1-SNAPSHOT" &&
./mvnw -pl :trino-test-jdbc-compatibility-old-driver versions:set-property -Dproperty="dep.presto-jdbc-under-test" -DnewVersion="${NEW}-cork-1-SNAPSHOT" &&
find -name pom.xml.versionsBackup -delete &&
git commit -a -m "Bump Cork version after code sync with Trino ${NEW}"
```

### update the update PR

Push the ready `update/cork/trino-${NEW}` branch to `origin` remote.

```shell
git push origin "update/cork/trino-${NEW}" --force-with-lease
```

### test out the PR on CI and iterate

If the CI fails on the Update PR, make relevant fixes, updating the fixup commits

### check OSS release notes

Read OSS release notes for versions OLD..NEW. Look out for any potential breaking changes. They are not guaranteed to be called out as using
the word "breaking" (or any other particular word), so read and try to understand every release notes bullet point.

```shell
for v in $(seq "$[OLD+1]" "${NEW}"); do
    open "https://trino.io/docs/current/release/release-${v}.html"
done
```
