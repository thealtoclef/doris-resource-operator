#!/bin/bash
# This script synchronizes the version from version.txt to both Helm charts

set -e

# Get the script directory and the repository root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." &>/dev/null && pwd)"

# Define the version file and chart paths
VERSION_FILE="${REPO_ROOT}/version.txt"
OPERATOR_CHART="${REPO_ROOT}/helm-charts/doris-resource-operator/Chart.yaml"
MANAGER_CHART="${REPO_ROOT}/helm-charts/doris-resource-manager/Chart.yaml"

# Check if version.txt exists
if [ ! -f "${VERSION_FILE}" ]; then
    echo "Error: ${VERSION_FILE} not found!"
    exit 1
fi

# Read version from version.txt
VERSION=$(cat "${VERSION_FILE}" | tr -d '[:space:]')

echo "Current version: ${VERSION}"

# Update operator chart version
if [ -f "${OPERATOR_CHART}" ]; then
    echo "Updating ${OPERATOR_CHART}..."
    # Use sed to update both version and appVersion
    sed -i '' "s/^version:.*$/version: \"${VERSION}\"/" "${OPERATOR_CHART}"
    sed -i '' "s/^appVersion:.*$/appVersion: \"${VERSION}\"/" "${OPERATOR_CHART}"
else
    echo "Warning: ${OPERATOR_CHART} not found!"
fi

# Update manager chart version
if [ -f "${MANAGER_CHART}" ]; then
    echo "Updating ${MANAGER_CHART}..."
    # Use sed to update both version and appVersion
    sed -i '' "s/^version:.*$/version: \"${VERSION}\"/" "${MANAGER_CHART}"
    sed -i '' "s/^appVersion:.*$/appVersion: \"${VERSION}\"/" "${MANAGER_CHART}"
else
    echo "Warning: ${MANAGER_CHART} not found!"
fi

echo "Version synchronization complete!"
echo "All charts now at version ${VERSION}"