#!/bin/bash
set -euo pipefail

function list_installed_packages()
{
    apt list --installed "$1" 2>/dev/null | awk -F'/' 'NR>1{print $1}'
}

function free_up_disk_space_ubuntu()
{
    local packages=(
        'azure-cli'
        'aspnetcore-*'
        'firefox*'
        'google-chrome-*'
        'google-cloud-*'
        'libmono-*'
        'llvm-*'
        'mysql-server-core-*'
        'powershell*'
        'microsoft-edge*')

    for package in "${packages[@]}"; do
        mapfile -t installed_packages < <(list_installed_packages "${package}")
        if [ ${#installed_packages[@]} -eq 0 ]; then
            echo "No packages matched by pattern ${package}"
        else
            echo "Removing packages by pattern ${package}: ${installed_packages[*]}"
            sudo apt-get --auto-remove -y purge "${installed_packages[@]}"
        fi
    done

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
free_up_disk_space_ubuntu
echo "::endgroup::"

echo "Disk space usage after cleaning:"
df -k .

