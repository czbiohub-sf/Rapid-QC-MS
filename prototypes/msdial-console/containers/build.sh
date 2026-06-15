#!/bin/bash
# Build the MS-DIAL Console Apptainer image.
#
# Prerequisites:
#   - The MS-DIAL fork must be built for linux-x64 first:
#       cd /hpc/mydata/anthony.goering/repos/msdial-fork
#       dotnet publish ./src/MSDIAL4/MsdialConsoleAppCore/MsdialConsoleAppCore.csproj \
#         --configuration "Release vendor unsupported" \
#         -p:DebugType=None --runtime linux-x64 --framework net8 \
#         --source ./Assemblies --source https://api.nuget.org/v3/index.json \
#         -o ./build/console-linux-x64
#
# Usage:
#   cd prototypes/msdial-console/containers
#   bash build.sh
#
# The build copies the published binary directory into the image at /opt/msdial/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MSDIAL_BUILD="/hpc/mydata/anthony.goering/repos/msdial-fork/build/console-linux-x64"

if [ ! -f "$MSDIAL_BUILD/MsdialConsoleApp" ]; then
    echo "ERROR: MS-DIAL binary not found at $MSDIAL_BUILD/MsdialConsoleApp"
    echo "Build the fork first (see header comments in this script)."
    exit 1
fi

# Stage the build output next to the .def file so %files can reference it
STAGING="$SCRIPT_DIR/MsdialConsoleApp_linux-x64"
rm -rf "$STAGING"
cp -r "$MSDIAL_BUILD" "$STAGING"

echo "Building Apptainer image..."
apptainer build "$SCRIPT_DIR/msdial-console.sif" "$SCRIPT_DIR/msdial-console.def"

# Clean up staging
rm -rf "$STAGING"

echo "Done: $SCRIPT_DIR/msdial-console.sif"
