#!/bin/bash

# Set the target directory
TARGET_DIR=bin/binary

# Check if the zip file exists; if so, delete it
if [ -f "${TARGET_DIR}.zip" ]; then
    rm "${TARGET_DIR}.zip"
fi

# Check if the backend directory exists; if so, delete it
if [ -d "$TARGET_DIR" ]; then
    rm -rf "$TARGET_DIR"
fi

# Recreate the directory
mkdir -p "$TARGET_DIR"

# Copy all files (including hidden files) from backend to the target directory
cp -a ./backend/. "$TARGET_DIR/"

# Print success message
echo "The folder has been recreated and the contents, including hidden files, have been copied successfully!"

# Navigate into the target directory
cd "$TARGET_DIR" || exit 1  # Exit if changing directory fails

# Print the current directory (optional)
pwd

echo "Deleting environments..."
rm -rf venv

echo "Deleting unwanted files..."
rm -rf logs/logs.log
rm -rf docs
rm -rf htmlcov
rm -rf dockerfile.sphinx
rm -rf sphinx_docs.sh
rm -rf test1
rm -rf build
rm -rf utils/dist
chmod 777 logs

echo "Deleting unwanted lines from .dockerignore...c"
sed -i '' '/\*.pyc/d' .dockerignore
sed -i '' '/\*.pyo/d' .dockerignore
sed -i '' '/\*.pyd/d' .dockerignore
sed -i '' '/__pycache__/d' .dockerignore
echo "Unwanted lines deleted successfully!"

# Compile Python files to .pyc
python -m compileall .

# Move each .pyc file out of __pycache__ into its corresponding directory
find . -name "*.pyc" -exec sh -c '
    for pyc_file do
        # Get the directory containing the __pycache__
        parent_dir=$(dirname "$pyc_file")
        
        # Create target directory if not exists
        target_dir=${parent_dir/__pycache__/}
        mkdir -p "$target_dir"
        
        # Move the .pyc file into its corresponding target directory
        mv "$pyc_file" "$target_dir/$(basename "$pyc_file" | sed "s/\.cpython-312//")"
    done
' sh {} +

# Delete the __pycache__ directories and original .py files
rm -rf $(find . -name "__pycache__")
find . -name "*.py" -type f ! -name "setup.py" -delete

echo "Conversion process to binary for the backend completed!"

echo "Conversion process to binary for the frontend completed!"

cd ../.. || exit 1  # Exit if changing directory fails

# Create the zip file containing all files (including hidden files) from both directories
zip -r "${TARGET_DIR}.zip" "${TARGET_DIR}/."

echo "The contents have been zipped into $TARGET_DIR.zip"