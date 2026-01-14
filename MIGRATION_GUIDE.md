# Migration Guide: R Support in Texera

## Overview

Starting with Texera version X.X.X, R language support has been moved to a separate plugin repository due to licensing considerations. The R plugin depends on `rpy2`, which is licensed under GPLv2, while the main Texera project uses Apache License 2.0.

This guide will help you migrate existing R workflows to use the new plugin architecture.

## What Changed?

### Before (Old Architecture)
- R executors were part of the main Texera codebase
- rpy2 was listed in `r-requirements.txt` in the main repo
- R support was always installed with Texera

### After (New Architecture)  
- R executors are in a separate plugin: `texera-r-plugin`
- Plugin is **optional** and installed separately
- Main Texera repo remains Apache-2.0 compliant

## Migration Steps

### For Users

#### 1. Update Your Installation

If you're using R operators in your workflows, install the R plugin:

```bash
# Remove old rpy2 installation (if installed)
pip uninstall rpy2 rpy2-arrow -y

# Install the R plugin
pip install git+https://github.com/Texera/texera-r-plugin.git
```

#### 2. Verify R is Working

Test that R operators still work:

```python
# This should now import from texera_r plugin
python -c "from texera_r import RTupleExecutor; print('R plugin installed successfully!')"
```

#### 3. Update Your Workflows

**Good news**: No changes needed! Your existing R workflows will continue to work exactly as before. The plugin provides the same API.

### For Developers

#### 1. Update Dependencies

**In your main Texera installation:**

```bash
cd /path/to/texera
pip install -r amber/requirements.txt  # Main dependencies

# Only install R plugin if you need R support
pip install git+https://github.com/Texera/texera-r-plugin.git
```

#### 2. Update Imports (if applicable)

If you have custom code that imports R executors:

**Old (will fail):**
```python
from core.models.RTupleExecutor import RTupleExecutor
from core.models.RTableExecutor import RTableExecutor
```

**New (correct):**
```python
try:
    from texera_r import RTupleExecutor, RTableExecutor
except ImportError:
    print("R plugin not installed. Install with: pip install git+https://github.com/Texera/texera-r-plugin.git")
```

#### 3. Docker Deployments

Update your Dockerfiles to optionally install the R plugin:

```dockerfile
# Install main Texera dependencies
RUN pip install -r amber/requirements.txt

# Optional: Install R plugin for R operator support
# Comment out the following line to disable R support
RUN pip install git+https://github.com/Texera/texera-r-plugin.git@main
```

### For Admins

#### 1. System-Wide Installation

If you're deploying Texera for multiple users who need R support:

```bash
# As root or in your deployment script
pip install git+https://github.com/Texera/texera-r-plugin.git

# Verify installation
python -c "import texera_r; print(f'R plugin version: {texera_r.__version__}')"
```

#### 2. Environment Variables

R plugin requires the same environment variables as before:

```bash
# Set R_HOME if not auto-detected
export R_HOME=/usr/lib/R  # Or your R installation path

# Optional: Storage configuration for LargeBinary support
export STORAGE_S3_ENDPOINT=http://localhost:9000
export STORAGE_S3_REGION=us-west-2
export STORAGE_S3_AUTH_USERNAME=texera_minio
export STORAGE_S3_AUTH_PASSWORD=password
```

## Troubleshooting

### "R operators require the texera-r-plugin package"

**Symptom:** When running R operators, you see an error about missing texera-r-plugin.

**Solution:**
```bash
pip install git+https://github.com/Texera/texera-r-plugin.git
```

### "ModuleNotFoundError: No module named 'texera_r'"

**Symptom:** The plugin is not found in your Python environment.

**Solution:**
- Verify you're using the correct Python environment
- Re-install the plugin: `pip install git+https://github.com/Texera/texera-r-plugin.git`
- Check with: `pip list | grep texera-r-plugin`

### R_HOME Not Found

**Symptom:** rpy2 cannot find your R installation.

**Solution:**
```bash
# Find R installation
R RHOME

# Set environment variable
export R_HOME=/path/to/R  # Use output from above command
```

### Import Errors in Custom Code

**Symptom:** Your custom operator code fails to import R executors.

**Solution:** Update imports from `core.models.RTupleExecutor` to `texera_r`.

## Backwards Compatibility

### Workflow Compatibility
- ✅ **All existing R workflows are fully compatible**
- ✅ R UDF operators work identically
- ✅ R Source operators work identically  
- ✅ No workflow modifications needed

### API Compatibility
- ✅ **Complete API compatibility**
- ✅ `RTupleExecutor` class interface unchanged
- ✅ `RTableExecutor` class interface unchanged
- ✅ All methods and parameters identical

### Breaking Changes
- ❌ **Import paths changed**: Must import from `texera_r` instead of `core.models`
- ❌ **Separate installation required**: Plugin must be explicitly installed
- ❌ **License change**: Plugin is GPLv2 (main Texera is Apache-2.0)

## Benefits of the New Architecture

1. **License Compliance**: Main Texera repo stays Apache-2.0 compliant
2. **Optional Feature**: Users who don't need R don't install it
3. **Independent Versioning**: R plugin can be updated independently
4. **Clearer Licensing**: Clear separation of GPL vs Apache code
5. **Same Performance**: No performance degradation (no network calls)

## Timeline

- **Version X.X.X**: R plugin architecture introduced
- **Supported until**: Old architecture deprecated in version X+1.X.X
- **Removed in**: Old architecture completely removed in version X+2.X.X

## Getting Help

- **Issues**: [https://github.com/Texera/texera-r-plugin/issues](https://github.com/Texera/texera-r-plugin/issues)
- **Main Texera Issues**: [https://github.com/Texera/texera/issues](https://github.com/Texera/texera/issues)
- **Documentation**: [https://github.com/Texera/texera-r-plugin](https://github.com/Texera/texera-r-plugin)

## FAQ

**Q: Do I need to modify my existing R workflows?**  
A: No, existing workflows work without modification.

**Q: Will performance be affected?**  
A: No, R still executes in-process. There's no network overhead.

**Q: Can I still use the old installation method?**  
A: No, the old R executors have been removed from the main repo.

**Q: Is the R plugin mandatory?**  
A: No, it's optional. Only install if you need R operator support.

**Q: What about Python operators?**  
A: Python operators remain in the main Texera repo (Apache-2.0).

**Q: Can I contribute to the R plugin?**  
A: Yes! Contributions welcome at [texera-r-plugin](https://github.com/Texera/texera-r-plugin).
