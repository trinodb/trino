#!/usr/bin/env node
'use strict';

const LABEL = 'cla-signed';
const COMMENT_MARKER = '<!-- trino-cla-check -->';
const CONTRIBUTORS_OWNER = 'trinodb';
const CONTRIBUTORS_REPO = 'cla';
const CONTRIBUTORS_PATH = 'contributors';
const NO_CLA_MESSAGE = "Thank you for your pull request and welcome to the Trino community. We require contributors to sign our [Contributor License Agreement](https://github.com/trinodb/cla/raw/master/Trino%20Foundation%20Individual%20CLA.pdf), and we don't seem to have you on file. Continue to work with us on the review and improvements in this PR, and submit the signed CLA to cla@trino.io. Photos, scans, or digitally-signed PDF files are all suitable. Processing may take a few days. The CLA needs to be on file before we merge your changes. For more information, see https://github.com/trinodb/cla";
const MISSING_EMAIL_MESSAGE = `Thank you for your pull request and welcome to our community. We could not parse the GitHub identity of the following contributors: **{{unidentifiedUsers}}**.
This is most likely caused by a git client misconfiguration; please make sure to:
1. check if your git client is configured with an email to sign commits \`git config --list | grep email\`
2. If not, set it up using \`git config --global user.email email@example.com\`
3. Make sure that the git commit email is configured in your GitHub account settings, see https://github.com/settings/emails`;
const TRUSTED_COMMENT_ASSOCIATIONS = new Set(['COLLABORATOR', 'MEMBER', 'OWNER']);

function sortedUnique(values) {
    return [...new Set(values.filter(Boolean))].sort((left, right) => String(left).localeCompare(String(right)));
}

async function resolveCheckTarget(check, eventName, event) {
    if (eventName === 'pull_request_target') {
        return {
            pullRequests: [event.pull_request],
            updatePullRequests: true,
        };
    }

    if (eventName === 'issue_comment') {
        if (!event.issue?.pull_request || !commentRequestsClaCheck(event.comment?.body || '')) {
            console.log('Issue comment does not request a CLA re-check; skipping');
            return null;
        }
        if (!TRUSTED_COMMENT_ASSOCIATIONS.has(event.comment?.author_association)) {
            console.log(`Ignoring CLA re-check from untrusted association: ${event.comment?.author_association || 'NONE'}`);
            return null;
        }
        const {data: pullRequest} = await check.github.rest.pulls.get({
            ...check.repo,
            pull_number: event.issue.number,
        });
        return {
            pullRequests: [pullRequest],
            updatePullRequests: true,
        };
    }

    if (eventName === 'merge_group') {
        return {
            mergeGroup: event.merge_group,
            pullRequests: [],
            updatePullRequests: false,
        };
    }

    console.log(`Unsupported event ${eventName}; skipping`);
    return null;
}

function commentRequestsClaCheck(comment) {
    const command = comment.trim();
    return command === '@cla-bot check' || command === '/cla-check';
}

function addJobErrorAnnotation(check, title, message) {
    check.core.error(message, {title});
}

function addClaFailureAnnotation(check, result) {
    const missing = result.unidentified || result.missing;
    const summary = result.unidentified
        ? 'Could not identify contributors'
        : 'CLA is missing for contributors';
    const message = `${summary}: ${missing.join(', ')}\n${result.message}`;

    addJobErrorAnnotation(check, 'Contributor License Agreement', message);
}

async function getContributors(check) {
    const {data: content} = await check.github.rest.repos.getContent({
        owner: CONTRIBUTORS_OWNER,
        repo: CONTRIBUTORS_REPO,
        path: CONTRIBUTORS_PATH,
    });
    if (content.encoding !== 'base64') {
        throw new Error(`Unsupported contributors encoding: ${content.encoding}`);
    }
    return JSON.parse(Buffer.from(content.content, 'base64').toString('utf8'));
}

async function listPullRequestCommits(check, pullRequest) {
    return listComparisonCommits(check, pullRequest.base.sha, pullRequest.head.sha);
}

async function listMergeGroupCommits(check, mergeGroup) {
    return listComparisonCommits(check, mergeGroup.base_sha, mergeGroup.head_sha || check.context.sha);
}

async function listComparisonCommits(check, baseSha, headSha) {
    if (!baseSha || !headSha) {
        throw new Error('Could not determine commit range for CLA check');
    }

    const commits = [];
    let totalCommits;
    for (let page = 1; ; page++) {
        const {data: comparison} = await check.github.rest.repos.compareCommitsWithBasehead({
            ...check.repo,
            basehead: `${baseSha}...${headSha}`,
            per_page: 100,
            page,
        });
        totalCommits = comparison.total_commits;
        commits.push(...comparison.commits);

        if (commits.length >= totalCommits || comparison.commits.length < 100) {
            break;
        }
    }

    if (commits.length !== totalCommits) {
        throw new Error(`Expected ${totalCommits} commits for ${baseSha}...${headSha}, but fetched ${commits.length}`);
    }
    return commits;
}

function findMissingClaContributors(commits, contributors) {
    const unidentified = sortedUnique(
        commits
            .map(commit => commitContributor(commit))
            .filter(contributor => !contributor.login && !contributor.email)
            .map(contributor => contributor.name || contributor.sha)
    );
    if (unidentified.length > 0) {
        return {
            passed: false,
            unidentified,
            message: MISSING_EMAIL_MESSAGE.replace('{{unidentifiedUsers}}', unidentified.join(', ')),
        };
    }

    const lowerCaseContributors = contributors.map(contributor => String(contributor).toLowerCase());
    const exactEmails = new Set(lowerCaseContributors.filter(contributor => contributor.includes('@') && !contributor.startsWith('@')));
    const domains = new Set(lowerCaseContributors.filter(contributor => contributor.startsWith('@')));
    const logins = new Set(lowerCaseContributors.filter(contributor => !contributor.includes('@')));

    const missing = sortedUnique(commits
        .map(commit => commitContributor(commit))
        .filter(contributor => !hasSignedCla(contributor, exactEmails, domains, logins))
        .map(contributor => contributor.login || contributor.email));

    return {
        passed: missing.length === 0,
        missing,
        message: NO_CLA_MESSAGE,
    };
}

function commitContributor(commit) {
    return {
        email: commit.commit?.author?.email,
        login: commit.author?.login,
        name: commit.commit?.author?.name,
        sha: commit.sha,
    };
}

function hasSignedCla(contributor, exactEmails, domains, logins) {
    if (contributor.email) {
        const email = contributor.email.toLowerCase();
        if (exactEmails.has(email)) {
            return true;
        }
        const domainStart = email.indexOf('@');
        if (domainStart !== -1 && domains.has(email.substring(domainStart))) {
            return true;
        }
    }
    return contributor.login && logins.has(contributor.login.toLowerCase());
}

async function addLabel(check, issueNumber) {
    await check.github.rest.issues.addLabels({
        ...check.repo,
        issue_number: issueNumber,
        labels: [LABEL],
    });
}

async function addLabels(check, issueNumbers) {
    await Promise.all(issueNumbers.map(issueNumber => addLabel(check, issueNumber)));
}

async function removeLabel(check, issueNumber) {
    try {
        await check.github.rest.issues.removeLabel({
            ...check.repo,
            issue_number: issueNumber,
            name: LABEL,
        });
    }
    catch (error) {
        if (error.status !== 404) {
            throw error;
        }
    }
}

async function removeLabels(check, issueNumbers) {
    await Promise.all(issueNumbers.map(issueNumber => removeLabel(check, issueNumber)));
}

async function upsertFailureComment(check, issueNumber, body) {
    const markedBody = `${body}\n\n${COMMENT_MARKER}`;

    const comments = await check.github.paginate(check.github.rest.issues.listComments, {
        ...check.repo,
        issue_number: issueNumber,
        per_page: 100,
    });
    const existingComment = comments.find(comment => comment.user?.type === 'Bot' && comment.body?.includes(COMMENT_MARKER));
    if (existingComment) {
        await check.github.rest.issues.updateComment({
            ...check.repo,
            comment_id: existingComment.id,
            body: markedBody,
        });
        return;
    }

    await check.github.rest.issues.createComment({
        ...check.repo,
        issue_number: issueNumber,
        body: markedBody,
    });
}

async function upsertFailureComments(check, issueNumbers, body) {
    await Promise.all(issueNumbers.map(issueNumber => upsertFailureComment(check, issueNumber, body)));
}

async function deleteFailureComment(check, issueNumber) {
    const comments = await check.github.paginate(check.github.rest.issues.listComments, {
        ...check.repo,
        issue_number: issueNumber,
        per_page: 100,
    });
    const existingComment = comments.find(comment => comment.user?.type === 'Bot' && comment.body?.includes(COMMENT_MARKER));
    if (!existingComment) {
        return;
    }
    await check.github.rest.issues.deleteComment({
        ...check.repo,
        comment_id: existingComment.id,
    });
}

async function deleteFailureComments(check, issueNumbers) {
    await Promise.all(issueNumbers.map(issueNumber => deleteFailureComment(check, issueNumber)));
}

async function checkCla({github, context, core}) {
    const check = {
        github,
        context,
        core,
        repo: context.repo,
    };
    const eventName = context.eventName;
    const event = context.payload;
    const target = await resolveCheckTarget(check, eventName, event);
    if (!target) {
        return;
    }

    const [commits, contributors] = await Promise.all([
        listTargetCommits(check, target),
        getContributors(check),
    ]);
    const result = findMissingClaContributors(commits, contributors);

    if (result.passed) {
        await bestEffortUpdatePullRequests(() => markTargetPassed(check, target));
        console.log(`All commit authors for ${describeTarget(check, target)} have signed the CLA`);
        return;
    }

    await bestEffortUpdatePullRequests(() => markTargetFailed(check, target, result.message));
    const missing = result.unidentified || result.missing;
    const failureMessage = `CLA check failed for ${describeTarget(check, target)}: ${missing.join(', ')}`;
    addClaFailureAnnotation(check, result);
    console.error(failureMessage);
    process.exitCode = 1;
}

async function listTargetCommits(check, target) {
    if (target.mergeGroup) {
        return listMergeGroupCommits(check, target.mergeGroup);
    }

    const commitsByPullRequest = await Promise.all(target.pullRequests.map(pullRequest => listPullRequestCommits(check, pullRequest)));
    const allCommits = commitsByPullRequest.flat();
    return sortedUnique(allCommits.map(commit => commit.sha))
        .map(sha => allCommits.find(commit => commit.sha === sha));
}

async function bestEffortUpdatePullRequests(update) {
    try {
        await update();
    }
    catch (error) {
        console.error('Failed to update CLA labels/comments', error);
    }
}

async function markTargetPassed(check, target) {
    if (!target.updatePullRequests) {
        return;
    }

    const issueNumbers = target.pullRequests.map(pullRequest => pullRequest.number);
    await addLabels(check, issueNumbers);
    await deleteFailureComments(check, issueNumbers);
}

async function markTargetFailed(check, target, message) {
    if (!target.updatePullRequests) {
        return;
    }

    const issueNumbers = target.pullRequests.map(pullRequest => pullRequest.number);
    await removeLabels(check, issueNumbers);
    await upsertFailureComments(check, issueNumbers, message);
}

function describeTarget(check, target) {
    if (target.mergeGroup) {
        return `merge group ${target.mergeGroup.head_ref || target.mergeGroup.head_sha || check.context.sha}`;
    }
    return target.pullRequests.map(pullRequest => `#${pullRequest.number}`).join(', ');
}

module.exports = checkCla;
