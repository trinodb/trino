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
        'mysql-server-core-*'
        'powershell*')

    sudo apt-get --auto-remove -y purge "${packages[@]}"

    echo "Autoremoving packages"
    sudo apt-get autoremove -y

    echo "Autocleaning"
    sudo apt-get autoclean -y

    echo  "Removing toolchains"
    sudo rm -rf \
      /usr/local/graalvm \
      /usr/local/lib/android/ \
      /usr/share/dotnet/ \
      /opt/ghc/ \
      /usr/local/share/boost/ \
      "${AGENT_TOOLSDIRECTORY}"

      echo "Prune docker images"
      sudo docker system prune --all -f
}

echo "Disk space usage before cleaning:"
df -k .

echo "::group::Clearing up disk usage"
time free_up_disk_space_ubuntu
echo "::endgroup::"

echo "Disk space usage after cleaning:"
df -k .
