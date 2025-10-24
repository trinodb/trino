#!/bin/bash
set -euo pipefail

function free_up_disk_space_ubuntu()
{
    local packages=(
        'azure-cli'
        'aspnetcore-*'
        'firefox*'
        'google-chrome-*'
        'libmono-*'
        'llvm-*'
        'man-db'
        'mysql-server-core-*'
        'powershell*')

    sudo apt-get --auto-remove -y purge "${packages[@]}"

    echo "Autoremoving packages"
    sudo apt-get autoremove -y

    echo "Autocleaning"
    sudo apt-get autoclean -y

    echo  "Removing toolchains"
    directories_to_be_removed=(
      "/usr/local/graalvm/"
      "/usr/local/lib/android/"
      "/usr/share/dotnet/"
      "/opt/ghc/"
      "/usr/local/share/boost/"
      "${AGENT_TOOLSDIRECTORY}")

    delete_directories_with_rsync "${directories_to_be_removed[@]}"

    echo "Prune docker images"
    sudo docker system prune --all -f
}

function delete_directories_with_rsync()
{
    sudo mkdir /tmp/empty
    for dir in "$@"; do
        echo "Deleting contents of $dir using rsync"
        sudo rsync --delete -a /tmp/empty/ "$dir"
        sudo rmdir "$dir"
    done
    sudo rmdir /tmp/empty
}

echo "Disk space usage before cleaning:"
df -k .

echo "::group::Clearing up disk usage"
time free_up_disk_space_ubuntu
echo "::endgroup::"

echo "Disk space usage after cleaning:"
df -k .
