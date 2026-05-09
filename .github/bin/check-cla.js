#!/usr/bin/env node
'use strict';

const fs = require('node:fs');

const API_URL = 'https://api.github.com';
const STATUS_CONTEXT = 'verification/cla-signed';
const LABEL = 'cla-signed';
const COMMENT_MARKER = '<!-- trino-cla-check -->';
const CONTRIBUTORS_URL = '/repos/trinodb/cla/contents/contributors';
const NO_CLA_MESSAGE = "Thank you for your pull request and welcome to the Trino community. We require contributors to sign our [Contributor License Agreement](https://github.com/trinodb/cla/raw/master/Trino%20Foundation%20Individual%20CLA.pdf), and we don't seem to have you on file. Continue to work with us on the review and improvements in this PR, and submit the signed CLA to cla@trino.io. Photos, scans, or digitally-signed PDF files are all suitable. Processing may take a few days. The CLA needs to be on file before we merge your changes. For more information, see https://github.com/trinodb/cla";
const MISSING_EMAIL_MESSAGE = `Thank you for your pull request and welcome to our community. We could not parse the GitHub identity of the following contributors: **{{unidentifiedUsers}}**.
This is most likely caused by a git client misconfiguration; please make sure to:
1. check if your git client is configured with an email to sign commits \`git config --list | grep email\`
2. If not, set it up using \`git config --global user.email email@example.com\`
3. Make sure that the git commit email is configured in your GitHub account settings, see https://github.com/settings/emails`;
const TRUSTED_COMMENT_ASSOCIATIONS = new Set(['COLLABORATOR', 'MEMBER', 'OWNER']);

const token = process.env.GITHUB_TOKEN || process.env.GH_TOKEN;
const dryRun = process.env.CLA_DRY_RUN === 'true';
const repository = process.env.GITHUB_REPOSITORY || 'trinodb/trino';
const [owner, repo] = repository.split('/');

if (!owner || !repo) {
    throw new Error(`Invalid GITHUB_REPOSITORY: ${repository}`);
}

function sortedUnique(values) {
    return [...new Set(values.filter(Boolean))].sort((left, right) => String(left).localeCompare(String(right)));
}

function readEvent() {
    if (!process.env.GITHUB_EVENT_PATH) {
        throw new Error('GITHUB_EVENT_PATH is not set');
    }
    return JSON.parse(fs.readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8'));
}

async function githubRequest(path, options = {}) {
    if (!token) {
        throw new Error('GITHUB_TOKEN is not set');
    }

    const response = await fetch(`${API_URL}${path}`, {
        method: options.method || 'GET',
        headers: {
            'Accept': 'application/vnd.github+json',
            'Authorization': `Bearer ${token}`,
            'User-Agent': 'trinodb-cla-check',
            'X-GitHub-Api-Version': '2022-11-28',
            ...options.headers,
        },
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });

    if (!response.ok) {
        const text = await response.text();
        throw new Error(`${options.method || 'GET'} ${path} failed: ${response.status} ${response.statusText}: ${text}`);
    }

    if (response.status === 204) {
        return null;
    }
    return response.json();
}

async function paginate(path) {
    const results = [];
    for (let page = 1; ; page++) {
        const separator = path.includes('?') ? '&' : '?';
        const pageResults = await githubRequest(`${path}${separator}per_page=100&page=${page}`);
        results.push(...pageResults);
        if (pageResults.length < 100) {
            return results;
        }
    }
}

async function resolveCheckTarget(eventName, event) {
    if (eventName === 'pull_request_target') {
        return {
            statusSha: event.pull_request.head.sha,
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
        const pullRequest = await githubRequest(`/repos/${owner}/${repo}/pulls/${event.issue.number}`);
        return {
            statusSha: pullRequest.head.sha,
            pullRequests: [pullRequest],
            updatePullRequests: true,
        };
    }

    if (eventName === 'merge_group') {
        return {
            statusSha: event.merge_group.head_sha || process.env.GITHUB_SHA,
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

async function setStatus(sha, state, description) {
    const targetUrl = process.env.GITHUB_SERVER_URL && process.env.GITHUB_REPOSITORY && process.env.GITHUB_RUN_ID
        ? `${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`
        : undefined;

    const status = {
        state,
        context: STATUS_CONTEXT,
        description,
        ...(targetUrl ? {target_url: targetUrl} : {}),
    };

    if (dryRun) {
        console.log('Would set status:', status);
        return;
    }

    await githubRequest(`/repos/${owner}/${repo}/statuses/${sha}`, {
        method: 'POST',
        body: status,
    });
}

async function getContributors() {
    const content = await githubRequest(CONTRIBUTORS_URL);
    if (content.encoding !== 'base64') {
        throw new Error(`Unsupported contributors encoding: ${content.encoding}`);
    }
    return JSON.parse(Buffer.from(content.content, 'base64').toString('utf8'));
}

async function listPullRequestCommits(pullRequest) {
    return listComparisonCommits(pullRequest.base.sha, pullRequest.head.sha);
}

async function listMergeGroupCommits(mergeGroup) {
    return listComparisonCommits(mergeGroup.base_sha, mergeGroup.head_sha || process.env.GITHUB_SHA);
}

async function listComparisonCommits(baseSha, headSha) {
    if (!baseSha || !headSha) {
        throw new Error('Could not determine commit range for CLA check');
    }

    const commits = [];
    let totalCommits;
    for (let page = 1; ; page++) {
        const comparison = await githubRequest(`/repos/${owner}/${repo}/compare/${baseSha}...${headSha}?per_page=100&page=${page}`);
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

async function addLabel(issueNumber) {
    if (dryRun) {
        console.log(`Would add label ${LABEL} to #${issueNumber}`);
        return;
    }
    await githubRequest(`/repos/${owner}/${repo}/issues/${issueNumber}/labels`, {
        method: 'POST',
        body: {labels: [LABEL]},
    });
}

async function addLabels(issueNumbers) {
    await Promise.all(issueNumbers.map(issueNumber => addLabel(issueNumber)));
}

async function removeLabel(issueNumber) {
    if (dryRun) {
        console.log(`Would remove label ${LABEL} from #${issueNumber}`);
        return;
    }
    try {
        await githubRequest(`/repos/${owner}/${repo}/issues/${issueNumber}/labels/${encodeURIComponent(LABEL)}`, {
            method: 'DELETE',
        });
    }
    catch (error) {
        if (!error.message.includes('404')) {
            throw error;
        }
    }
}

async function removeLabels(issueNumbers) {
    await Promise.all(issueNumbers.map(issueNumber => removeLabel(issueNumber)));
}

async function upsertFailureComment(issueNumber, body) {
    const markedBody = `${body}\n\n${COMMENT_MARKER}`;
    if (dryRun) {
        console.log(`Would upsert failure comment on #${issueNumber}:\n${markedBody}`);
        return;
    }

    const comments = await paginate(`/repos/${owner}/${repo}/issues/${issueNumber}/comments`);
    const existingComment = comments.find(comment => comment.user?.type === 'Bot' && comment.body?.includes(COMMENT_MARKER));
    if (existingComment) {
        await githubRequest(`/repos/${owner}/${repo}/issues/comments/${existingComment.id}`, {
            method: 'PATCH',
            body: {body: markedBody},
        });
        return;
    }

    await githubRequest(`/repos/${owner}/${repo}/issues/${issueNumber}/comments`, {
        method: 'POST',
        body: {body: markedBody},
    });
}

async function upsertFailureComments(issueNumbers, body) {
    await Promise.all(issueNumbers.map(issueNumber => upsertFailureComment(issueNumber, body)));
}

async function deleteFailureComment(issueNumber) {
    if (dryRun) {
        console.log(`Would delete marked failure comment from #${issueNumber}`);
        return;
    }

    const comments = await paginate(`/repos/${owner}/${repo}/issues/${issueNumber}/comments`);
    const existingComment = comments.find(comment => comment.user?.type === 'Bot' && comment.body?.includes(COMMENT_MARKER));
    if (!existingComment) {
        return;
    }
    await githubRequest(`/repos/${owner}/${repo}/issues/comments/${existingComment.id}`, {
        method: 'DELETE',
    });
}

async function deleteFailureComments(issueNumbers) {
    await Promise.all(issueNumbers.map(issueNumber => deleteFailureComment(issueNumber)));
}

async function main() {
    const eventName = process.env.GITHUB_EVENT_NAME;
    const event = readEvent();
    let target;
    try {
        target = await resolveCheckTarget(eventName, event);
        if (!target) {
            return;
        }
        if (!target.statusSha) {
            throw new Error('Could not determine commit SHA for CLA status');
        }

        await setStatus(target.statusSha, 'pending', 'Checking Contributor License Agreement');

        const [commits, contributors] = await Promise.all([
            listTargetCommits(target),
            getContributors(),
        ]);
        const result = findMissingClaContributors(commits, contributors);

        if (result.passed) {
            await setStatus(target.statusSha, 'success', 'All commit authors have signed the CLA');
            await bestEffortUpdatePullRequests(() => markTargetPassed(target));
            console.log(`All commit authors for ${describeTarget(target)} have signed the CLA`);
            return;
        }

        await setStatus(target.statusSha, 'failure', 'CLA is missing for one or more commit authors');
        await bestEffortUpdatePullRequests(() => markTargetFailed(target, result.message));
        const missing = result.unidentified || result.missing;
        console.error(`CLA check failed for ${describeTarget(target)}: ${missing.join(', ')}`);
        process.exitCode = 1;
        return;
    }
    catch (error) {
        if (target?.statusSha) {
            try {
                await setStatus(target.statusSha, 'error', 'CLA check could not complete');
            }
            catch (statusError) {
                console.error('Failed to set CLA error status', statusError);
            }
        }
        throw error;
    }
}

async function listTargetCommits(target) {
    if (target.mergeGroup) {
        return listMergeGroupCommits(target.mergeGroup);
    }

    const commitsByPullRequest = await Promise.all(target.pullRequests.map(pullRequest => listPullRequestCommits(pullRequest)));
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

async function markTargetPassed(target) {
    if (!target.updatePullRequests) {
        return;
    }

    const issueNumbers = target.pullRequests.map(pullRequest => pullRequest.number);
    await addLabels(issueNumbers);
    await deleteFailureComments(issueNumbers);
}

async function markTargetFailed(target, message) {
    if (!target.updatePullRequests) {
        return;
    }

    const issueNumbers = target.pullRequests.map(pullRequest => pullRequest.number);
    await removeLabels(issueNumbers);
    await upsertFailureComments(issueNumbers, message);
}

function describeTarget(target) {
    if (target.mergeGroup) {
        return `merge group ${target.mergeGroup.head_ref || target.statusSha}`;
    }
    return target.pullRequests.map(pullRequest => `#${pullRequest.number}`).join(', ');
}

main().catch(error => {
    console.error(error);
    process.exit(1);
});
