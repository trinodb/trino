#!/usr/bin/env bash

set -euo pipefail

log() {
    printf "[%s] %s\n" "$(date +%F" "%T)" "$1" >&2
}

err_exit() {
    log "$@"
    exit 1
}

elapsed_hours_from_aws_time() {
    local awsDateTime=$1
    local epoch

    # Handle empty input
    [ -n "${awsDateTime}" ] || err_exit "Empty AWS date time provided"

    # Handle different AWS date formats using case and grep for POSIX compatibility
    # Format 1: 2025-01-08T10:30:00+00:00 (timezone offset)
    # Format 2: 2025-01-08T10:30:00.000+00:00 (timezone offset with fractional seconds)
    # Format 3: 2025-01-08T10:30:00.000Z (UTC with milliseconds)
    # Format 4: 2025-01-08T10:30:00Z (UTC without milliseconds)

    if echo "${awsDateTime}" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?[+-][0-9]{2}:[0-9]{2}$'; then
        # Format with timezone offset like +00:00, with optional fractional seconds
        local cleanDateTime
        # Remove fractional seconds if present for better compatibility: 2025-01-08T10:30:00.947000+00:00 -> 2025-01-08T10:30:00+00:00
        case "${awsDateTime}" in
            *.*+*)
                # Extract parts: date.fractional+timezone
                local before_plus="${awsDateTime%+*}"
                local after_plus="${awsDateTime##*+}"
                local date_part="${before_plus%.*}"
                cleanDateTime="${date_part}+${after_plus}"
                ;;
            *.*-*)
                # Extract parts: date.fractional-timezone
                local before_minus="${awsDateTime%-*}"
                local after_minus="${awsDateTime##*-}"
                local date_part="${before_minus%.*}"
                cleanDateTime="${date_part}-${after_minus}"
                ;;
            *) cleanDateTime="${awsDateTime}" ;;
        esac
        # first call is for Linux, the following one for Mac
        epoch=$(date --date "${cleanDateTime}" +%s 2>/dev/null) ||
            epoch=$(date -j -f "%FT%T%z" "${cleanDateTime}" +%s 2>/dev/null)
    elif echo "${awsDateTime}" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z$'; then
        # Format with Z suffix (UTC), with or without fractional seconds
        local cleanDateTime
        # Remove fractional seconds if present: 2025-01-08T10:30:00.000Z -> 2025-01-08T10:30:00Z
        case "${awsDateTime}" in
            *.*Z) cleanDateTime="${awsDateTime%.*}Z" ;;
            *) cleanDateTime="${awsDateTime}" ;;
        esac
        # Convert Z to +0000 for better compatibility
        cleanDateTime="${cleanDateTime%Z}+0000"
        # first call is for Linux, the following one for Mac
        epoch=$(date --date "${cleanDateTime}" +%s 2>/dev/null) ||
            epoch=$(date -j -f "%FT%T%z" "${cleanDateTime}" +%s 2>/dev/null)
    else
        err_exit "Unexpected AWS date time format ${awsDateTime}"
    fi

    # Validate that we got a valid epoch time
    if [ -n "${epoch}" ] && echo "${epoch}" | grep -qE '^[0-9]+$'; then
        echo $((($(date +%s) - epoch) / 3600))
    else
        err_exit "Failed to parse AWS date time ${awsDateTime}"
    fi
}

elapsed_hours_from_millis_timestamp() {
    local millisTimestamp=$1

    [[ "${millisTimestamp}" =~ ^[0-9]+$ ]] || err_exit "Unexpected timestamp arg ${millisTimestamp}"
    echo $((($(date +%s) - (millisTimestamp / 1000)) / 3600))
}

get_user_confirmation() {
    read -p "Are you sure (enter strict 'yes' to proceed)? " -r
    [[ $REPLY =~ ^yes$ ]]
}

check_required_tools() {
    if ! command -v aws >/dev/null 2>&1; then
        err_exit "AWS CLI is required but not installed."
    fi

    if ! command -v jq >/dev/null 2>&1; then
        err_exit "jq is required but not installed."
    fi
}

usage() {
    cat >&2 <<EOF
Usage:
    $0 [--delete] [--auto-confirm] [--output-file <file>] [--delete-batch-limit <int>] [--name-substring <string>] [--age-hours <int>] [--region <string>]
Where options are:
    --delete - perform deletion
    --auto-confirm - automatic confirmation of deletion
    --output-file - output file with cluster identifiers qualified to delete. If given, it won't be deleted on exit
    --delete-batch-limit - limit number of clusters to be deleted at once
    --name-substring - substring that must be present in cluster name to qualify for deletion
    --age-hours - minimum age in hours for clusters to qualify for deletion
    --region - aws region
Description
    This script allows to delete AWS Redshift clusters that contain a specific substring in their name
    and are older than the specified number of hours.
    It is safe to run this script manually - it will analyze AWS Redshift and output results. It won't delete
    anything without strict user confirmation (with exception when run with --auto-confirm option!)
EOF
}

main() {
    while [ $# -gt 0 ]; do
        case "$1" in
        -h | --help)
            usage
            exit
            ;;
        --delete)
            delete=true
            shift
            ;;
        --auto-confirm)
            autoConfirm=true
            shift
            ;;
        --output-file)
            outputFile=$2
            shift 2
            ;;
        --delete-batch-limit)
            deleteBatchLimit=$2
            [[ "${deleteBatchLimit}" =~ ^[0-9]+$ && "${deleteBatchLimit}" -gt 0 ]] ||
                err_exit "Invalid --delete-batch-limit value ${deleteBatchLimit}"
            shift 2
            ;;
        --name-substring)
            nameSubstring=$2
            shift 2
            ;;
        --age-hours)
            ageHours=$2
            [[ "${ageHours}" =~ ^[0-9]+$ && "${ageHours}" -gt 0 ]] ||
                err_exit "Invalid --age-hours value ${ageHours}"
            shift 2
            ;;
        --region)
            region=$2
            shift 2
            ;;
        *)
            err_exit "Unknown argument given $1"
            ;;
        esac
    done

    # defaults if not provided
    delete=${delete:-false}
    autoConfirm=${autoConfirm:-false}
    deleteBatchLimit=${deleteBatchLimit:-10}
    nameSubstring=${nameSubstring:-""}
    ageHours=${ageHours:-24}
    region=${region:-"us-east-1"}

    # Validate required parameters
    [ -n "${nameSubstring}" ] || err_exit "Parameter --name-substring is required"

    # Check for required tools
    check_required_tools

    echo "Cleaning Redshift clusters..."
    echo "region: $region"
    echo "name-substring: $nameSubstring"
    echo "age-hours: $ageHours"

    find_redshift_clusters_to_delete
    [ "${clustersToDeleteCount}" -gt 0 ] || {
        log "Nothing to delete, exiting"
        exit 0
    }

    echo -e "\nList of Redshift clusters to be deleted:"
    cat "${outputFile}"

    "${delete}" || exit 0

    echo -e "\nYou are about to delete ${clustersToDeleteCount} Redshift clusters shown above!"
    echo "This action is IRREVERSIBLE and will permanently delete the clusters and their data!"

    if "${autoConfirm}" || get_user_confirmation; then
        delete_redshift_clusters "${outputFile}"
    else
        log "Deletion of Redshift clusters skipped"
    fi
}

qualify_for_deletion() {
    local inputClusters=$1
    local clusters
    local cluster
    local clusterIdentifier
    local clusterCreateTime
    local ageInHours

    readarray -t clusters < <(jq --compact-output '.[]' <<<"${inputClusters}")

    for cluster in "${clusters[@]}"; do
        clusterIdentifier=$(jq --raw-output '.ClusterIdentifier' <<<"${cluster}")
        clusterCreateTime=$(jq --raw-output '.ClusterCreateTime' <<<"${cluster}")

        # Check if cluster name contains the required substring
        if [[ "${clusterIdentifier}" != *"${nameSubstring}"* ]]; then
            log "Cluster ${clusterIdentifier} does not contain substring '${nameSubstring}', skipping"
            continue
        fi

        # Calculate age in hours
        ageInHours=$(elapsed_hours_from_aws_time "${clusterCreateTime}") || {
            log "Failed to calculate age for cluster ${clusterIdentifier}, skipping"
            continue
        }

        # Validate that ageInHours is a valid integer
        if [ -z "${ageInHours}" ] || ! echo "${ageInHours}" | grep -qE '^[0-9]+$'; then
            log "Invalid age calculation for cluster ${clusterIdentifier} (got: '${ageInHours}'), skipping"
            continue
        fi

        if [ "${ageInHours}" -lt "${ageHours}" ]; then
            log "Cluster ${clusterIdentifier} is only ${ageInHours} hours old, requires ${ageHours} hours minimum age"
            continue
        fi

        log "Cluster ${clusterIdentifier} qualified for deletion (age: ${ageInHours} hours)"
        echo "${clusterIdentifier}" >>"${outputFile}"
        ((clustersToDeleteCount += 1))

        [ "${clustersToDeleteCount}" -lt "${deleteBatchLimit}" ] || break
    done
}

find_redshift_clusters_to_delete() {
    local response
    local marker

    if [ -z ${outputFile+x} ]; then
        outputFile=$(mktemp)
        trap 'log "Clean up temp file" ; rm -f "${outputFile}"' EXIT
    fi

    [ -f "${outputFile}" ] && rm -f "${outputFile}"
    clustersToDeleteCount=0

    marker=""
    log "AWS API call redshift describe-clusters"
    while true; do
        # shellcheck disable=SC2046
        response=$(aws redshift describe-clusters \
            --tag-keys "project" \
            --tag-values "trino-redshift" \
            --region "$region" \
            --output json \
            $([ -n "${marker}" ] && echo --marker "${marker}")) ||
            err_exit "API call redshift describe-clusters failed"

        qualify_for_deletion "$(jq '.Clusters' <<<"${response}")" ||
            err_exit "Unexpected error while qualifying clusters for deletion"

        [ "${clustersToDeleteCount}" -lt "${deleteBatchLimit}" ] || break

        marker=$(jq '.Marker // empty' -r <<<"${response}")
        [ -n "${marker}" ] || break
        log "AWS API call redshift describe-clusters (next page)"
    done

    log "Found ${clustersToDeleteCount} Redshift clusters eligible for deletion"
}

delete_redshift_clusters() {
    local file=$1
    declare -a clustersToDelete

    [ -f "${file}" ] || err_exit "File not found ${file}"

    mapfile -t clustersToDelete < "${file}"

    log "Starting deletion of ${#clustersToDelete[@]} Redshift clusters"

    for clusterIdentifier in "${clustersToDelete[@]}"; do
        log "Deleting Redshift cluster: ${clusterIdentifier}"
        aws redshift delete-cluster \
            --cluster-identifier "${clusterIdentifier}" \
            --skip-final-cluster-snapshot \
            --region "$region" ||
            err_exit "Failed to delete cluster ${clusterIdentifier}"
        log "Successfully initiated deletion of cluster: ${clusterIdentifier}"
    done
}

main "$@"
