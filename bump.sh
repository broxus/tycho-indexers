#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# The git URL for tycho dependencies, used to identify the correct lines.
TYCHO_GIT_URL="https://github.com/broxus/tycho.git"
# The git URL for the tycho-types patch, used to identify the correct line.
TYCHO_TYPES_GIT_URL="https://github.com/broxus/tycho-types.git"


# --- Argument Validation ---
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: $0 <new_tycho_hash> [new_tycho_types_hash]"
    echo "  <new_tycho_hash>: The full git commit hash for tycho dependencies."
    echo "  [new_tycho_types_hash]: (Optional) The full git commit hash for the tycho-types patch."
    echo
    echo "Example: $0 096de11a1a5b82261a87d383165b45f475f4d433 8f81e3a1b3c8f2b7e6d4c1e9a2b5d4e7f8a9b0c1"
    exit 1
fi

NEW_TYCHO_HASH="$1"
NEW_TYCHO_TYPES_HASH=""
if [ "$#" -eq 2 ]; then
    NEW_TYCHO_TYPES_HASH="$2"
fi

# Use the first 8 characters of the hash for the version metadata.
SHORT_TYCHO_HASH=${NEW_TYCHO_HASH:0:8}

echo "New Tycho Hash: $NEW_TYCHO_HASH"
echo "New Short Tycho Hash for versions: $SHORT_TYCHO_HASH"
if [ -n "$NEW_TYCHO_TYPES_HASH" ]; then
    echo "New Tycho-Types Hash: $NEW_TYCHO_TYPES_HASH"
fi
echo "--------------------------------------------------"

# --- Main Logic ---
# Find all Cargo.toml files and process them one by one.
# Using -print0 and read -d is the safest way to handle filenames with spaces.
find . -name "Cargo.toml" -print0 | while IFS= read -r -d $'\0' cargo_file; do

    # Check if the file is a workspace root Cargo.toml
    if grep -q "\[workspace\]" "$cargo_file"; then
        echo "Processing workspace file: $cargo_file"

        # Update all tycho dependencies using their unique git URL
        # sed searches for lines containing the URL and then replaces the rev on that line.
        # The regex `[0-9a-f]{40}` matches a full 40-character SHA-1 hash.
        sed -i.bak -E "/${TYCHO_GIT_URL//\//\\/}/ s/rev = \"[0-9a-f]{40}\"/rev = \"$NEW_TYCHO_HASH\"/" "$cargo_file"
        echo "  -> Updated tycho dependencies rev."

        # If the second hash was provided, update the tycho-types patch
        if [ -n "$NEW_TYCHO_TYPES_HASH" ]; then
            sed -i.bak -E "/${TYCHO_TYPES_GIT_URL//\//\\/}/ s/rev = \"[0-9a-f]{40}\"/rev = \"$NEW_TYCHO_TYPES_HASH\"/" "$cargo_file"
            echo "  -> Updated tycho-types patch rev."
        fi

    # Check if the file is a package (member) Cargo.toml
    elif grep -q "\[package\]" "$cargo_file"; then
        # Check if the file has a version with our custom metadata
        if grep -q "version = \".*+tychov-.*\"" "$cargo_file"; then
            echo "Processing member file: $cargo_file"

            # Use sed with a capture group to update only the hash part of the version string.
            # (version = ".*+tychov-) captures the stable part of the version.
            # [0-9a-f]+ matches the old hash.
            # \1 is the backreference to the captured group.
            sed -i.bak -E "s/(version = \".*\+tychov-)[0-9a-f]+\"/\1$SHORT_TYCHO_HASH\"/" "$cargo_file"
            echo "  -> Updated version to use hash '$SHORT_TYCHO_HASH'."
        fi
    fi
done

# Clean up backup files created by sed
find . -name "Cargo.toml.bak" -delete

echo "--------------------------------------------------"
echo "✅ Version bump complete!"