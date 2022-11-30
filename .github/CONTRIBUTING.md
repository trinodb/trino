# Contributing to Trino


This is the process we suggest for contributions.  This process is designed to
reduce the burden on project reviewers, impact on other contributors, and to
keep the amount of rework from the contributor to a minimum.

By contributing to Trino, you agree that your contributions will be licensed
under the [Apache License Version 2.0 (APLv2)](../LICENSE).

Find more information at [the Trino site](https://trino.io/development/).

## Contributor License Agreement

In order to accept your pull request, we need you to [submit a contributor
license agreement (CLA)](https://github.com/trinodb/cla).

## Discussion

Start a discussion by creating a Github
[issue](https://github.com/trinodb/trino/issues), or asking on
[Slack](https://trino.io/slack.html) (unless the change is trivial).

* This step helps you identify possible collaborators and reviewers.
* Does the change align with technical vision and project values?
* Will the change conflict with another change in progress? If so, work with
  others to minimize impact.
* Is this change large?  If so, work with others to break into smaller steps.

## Implementation

Implement the change.

* If the change is large, post a preview Github
  [pull request](https://github.com/trinodb/trino/pulls) with the title
  prefixed with `[WIP]`, and share with collaborators.
* Include tests and documentation as necessary.


## Pull request

Create a Github [pull request](https://github.com/trinodb/trino/pulls) (PR).

* Make sure the pull request passes the tests in CI.
* If known, request a review from an expert in the area changed.  If unknown,
  ask for help on [Slack](https://trino.io/slack.html).

## Review

Review is performed by one or more reviewers.

* This normally happens within a few days, but may take longer if the change is
  large, complex, or if a critical reviewer is unavailable. Feel free to ping
  the reviewer on the pull request.

## Updates

Address concerns and update the pull request.

* Comments are addressed to each individual commit in the pull request, and
  changes should be addressed in a new
  [`fixup!` commit](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---fixupamendrewordltcommitgt)
  placed after each commit. This is to make it easier for the reviewer to see
  what was updated.
* After pushing the changes, add a comment to the pull-request, mentioning the
  reviewers by name, stating that the review comments have been addressed.
  This is the only way that a reviewer is notified that you are ready for the
  code to be reviewed again.
* Go to the [review](#review) step.

## Merge

Maintainer merges the pull request after final changes are accepted.
