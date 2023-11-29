#!/bin/bash
set -euo pipefail

function list_installed_packages()
{
    apt list --installed "$1" 2>/dev/null | awk -F'/' 'NR>1{print $1}' | tr '\n' ' '
}

function free_up_disk_space_ubuntu()
{
    local packages=(
        'azure-cli'
        'aspnetcore-*'
        'dotnet-*'
        'firefox*'
        'google-chrome-*'
        'google-cloud-*'
        'libmono-*'
        'llvm-*'
        'imagemagick'
        'postgresql-*'
        'rubu-*'
        'spinxsearch'
        'unixodbc-dev'
        'mercurial'
        'esl-erlang'
        'microsoft-edge-stable'
        'mono-*'
        'msbuild'
        'mysql-server-core-*'
        'php-*'
        'php7*'
        'powershell*'
        'mongo*'
        'microsoft-edge*'
        'subversion')

    for package in "${packages[@]}"; do
        installed_packages=$(list_installed_packages "${package}")
        echo "Removing packages by pattern ${package}: ${installed_packages}"
        sudo apt-get --auto-remove -y purge ${installed_packages}
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

echo "Clearing up disk usage:"
free_up_disk_space_ubuntu

echo "Disk space usage after cleaning:"
df -k .
