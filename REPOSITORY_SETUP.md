# Setting Up the texera-r-plugin Repository

This guide explains how to extract the `texera-r-plugin` directory into a separate Git repository.

## Current Structure

The plugin has been created at:
```
/Users/kunwoopark/workspace/fork/texera/texera-r-plugin/
```

This directory needs to be moved to its own GitHub repository.

## Steps to Create Separate Repository

### 1. Create New GitHub Repository

Go to GitHub and create a new repository:
- **Name**: `texera-r-plugin`
- **Organization**: Texera
- **Visibility**: Public
- **License**: GNU General Public License v2.0
- **Description**: R language support plugin for Apache Texera

### 2. Initialize the Separate Repository

```bash
# Move to the plugin directory
cd /Users/kunwoopark/workspace/fork/texera/texera-r-plugin

# Initialize git repository
git init

# Add all files
git add .

# Create initial commit
git commit -m "Initial commit: R plugin extracted from main Texera repository

This plugin provides R language support for Apache Texera.
Separated due to licensing (GPLv2 via rpy2 dependency).

Includes:
- RTupleExecutor and RTableExecutor
- R utility functions
- Setup for pip installation
- Documentation and migration guides
"

# Add remote (replace with actual URL)
git remote add origin https://github.com/Texera/texera-r-plugin.git

# Push to GitHub
git branch -M main
git push -u origin main
```

### 3. Update Main Texera Repository

In the main Texera repository:

```bash
cd /Users/kunwoopark/workspace/fork/texera

# The texera-r-plugin directory should be removed from main repo
# (Or added to .gitignore if keeping as reference)
rm -rf texera-r-plugin/  # After pushing to separate repo

# Or add to .gitignore
echo "texera-r-plugin/" >> .gitignore

# Commit changes to main repo
git add .
git commit -m "Remove R support code, now in separate plugin

R language support has been extracted to a separate repository
(texera-r-plugin) due to GPLv2 licensing requirements of rpy2.

Changes:
- Removed R executor files (RTupleExecutor, RTableExecutor, r_utils)
- Updated executor_manager.py to use optional texera_r plugin
- Updated r-requirements.txt with plugin installation instructions
- Added R plugin documentation to README.md
- Added migration guide (MIGRATION_R_PLUGIN.md)

The plugin is now optional and can be installed with:
pip install git+https://github.com/Texera/texera-r-plugin.git

See: https://github.com/Texera/texera-r-plugin
"
```

### 4. Configure GitHub Repository Settings

On GitHub (https://github.com/Texera/texera-r-plugin):

1. **Add Topics**: `r-language`, `texera`, `data-processing`, `workflow`, `plugin`
2. **Add Description**: "R language support plugin for Apache Texera (GPLv2)"
3. **Set License**: GNU General Public License v2.0
4. **Enable Issues**: For bug reports and feature requests
5. **Add README badges**: License, version, etc.

### 5. Publish to PyPI (Optional but Recommended)

For easier installation:

```bash
cd /path/to/texera-r-plugin

# Install build tools
pip install build twine

# Build distribution
python -m build

# Upload to PyPI (you'll need PyPI credentials)
python -m twine upload dist/*
```

After publishing to PyPI, users can install with:
```bash
pip install texera-r-plugin
```

### 6. Create GitHub Release

Create a release (v0.1.0) on GitHub:

1. Go to Releases → "Create a new release"
2. **Tag**: `v0.1.0`
3. **Title**: "Initial Release - R Plugin v0.1.0"
4. **Description**:
```markdown
## Initial Release

This is the first release of the Texera R Plugin, extracted from the main 
Texera repository to maintain license compliance.

### Features
- R UDF Operator support (Tuple API and Table API)
- R Source Operator support
- Arrow-based data transfer
- Large Binary object support via S3
- Full compatibility with Texera workflows

### Installation
```bash
pip install git+https://github.com/Texera/texera-r-plugin.git@v0.1.0
```

### License
GPLv2 (due to rpy2 dependency)

### Documentation
See README.md and MIGRATION_GUIDE.md for details.
```

## Repository Structure

After setup, the separate repository will have:

```
texera-r-plugin/
├── .gitignore
├── LICENSE (GPLv2)
├── README.md
├── MIGRATION_GUIDE.md
├── REPOSITORY_SETUP.md (this file)
├── setup.py
├── requirements.txt
└── texera_r/
    ├── __init__.py
    ├── RTupleExecutor.py
    ├── RTableExecutor.py
    └── r_utils.py
```

## Verification

Test the plugin installation:

```bash
# From GitHub
pip install git+https://github.com/Texera/texera-r-plugin.git

# Verify
python -c "import texera_r; print(f'Version: {texera_r.__version__}')"
python -c "from texera_r import RTupleExecutor, RTableExecutor; print('Success!')"
```

## Maintenance

### Updating the Plugin

To make changes:

```bash
# Clone the plugin repo
git clone https://github.com/Texera/texera-r-plugin.git
cd texera-r-plugin

# Make changes
# ...

# Commit and push
git add .
git commit -m "Description of changes"
git push origin main

# Create new release for version updates
git tag v0.1.1
git push origin v0.1.1
```

### Syncing with Main Texera

If Texera's core models change, you may need to update the plugin:

1. Check for breaking changes in Texera's operator interfaces
2. Update plugin code if needed
3. Test compatibility
4. Release new plugin version

## Links

- **Main Texera**: https://github.com/Texera/texera
- **Plugin Repository**: https://github.com/Texera/texera-r-plugin (to be created)
- **PyPI** (future): https://pypi.org/project/texera-r-plugin/
