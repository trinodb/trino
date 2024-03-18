set -exuo pipefail

# configure libaio
echo "8388608" > /proc/sys/fs/aio-max-nr

# disk for native
mkdir -p /opt/data
