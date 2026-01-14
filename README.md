# Texera R Plugin

This plugin provides R language support for [Apache Texera](https://github.com/Texera/texera).

## ⚠️ License Notice

This plugin is licensed under **GPLv2** due to its dependency on [rpy2](https://github.com/rpy2/rpy2), which is GPL-licensed. This is separate from the main Texera project which uses Apache License 2.0.

By using this plugin, you agree to the terms of the GPLv2 license.

## Installation

### Prerequisites

1. **R installation** - You must have R installed on your system
   - Download from: https://www.r-project.org/
   - Set `R_HOME` environment variable (if not auto-detected)

2. **Required R packages**:
   ```r
   install.packages(c("arrow", "coro", "aws.s3"))
   ```

### Install the Plugin

```bash
# Install from GitHub (recommended)
pip install git+https://github.com/Texera/texera-r-plugin.git

# Or install from PyPI (when available)
pip install texera-r-plugin

# For development (from source)
git clone https://github.com/Texera/texera-r-plugin.git
cd texera-r-plugin
pip install -e .
```

## Usage

Once installed, the plugin will be automatically detected by Texera. You can use R operators in your workflows:

### R UDF Operator (Tuple API)

```r
library(coro)
coro::generator(function(tuple, port) {
  # Process tuple and yield results
  tuple$new_column <- tuple$existing_column * 2
  yield(tuple)
})
```

### R UDF Operator (Table API)

```r
function(table, port) {
  # Process dataframe and return result
  table$new_column <- table$existing_column * 2
  return(table)
}
```

### R Source Operator

```r
library(coro)
coro::generator(function() {
  yield(list(text = "Hello from R!"))
  yield(list(text = "Second message"))
})
```

## Features

- **Tuple API**: Process data row-by-row with generators
- **Table API**: Process data in batches with dataframes
- **Arrow Integration**: Efficient data transfer using Apache Arrow
- **Large Binary Support**: Handle large binary objects via S3-compatible storage

## Compatibility

- **Texera**: Compatible with Texera 0.1.0+
- **Python**: 3.8, 3.9, 3.10, 3.11
- **R**: 4.0+
- **rpy2**: 3.5.11

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

### Building from Source

```bash
python setup.py sdist bdist_wheel
```

## Troubleshooting

### R_HOME not found

If you get an error about R_HOME not being set:

```bash
# Find your R installation
R RHOME

# Set environment variable (add to ~/.bashrc or ~/.zshrc)
export R_HOME=/usr/lib/R  # Use the output from above command
```

### Missing R packages

If R operators fail due to missing packages:

```r
# In R console
install.packages(c("arrow", "coro", "aws.s3"))
```

## License

This project is licensed under the **GNU General Public License v2.0** - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

- **Issues**: https://github.com/Texera/texera-r-plugin/issues
- **Main Texera Project**: https://github.com/Texera/texera
- **Documentation**: https://docs.texera.io

## Credits

This plugin is maintained by the Texera development team and uses:
- [rpy2](https://github.com/rpy2/rpy2) for Python-R interface
- [Apache Arrow](https://arrow.apache.org/) for efficient data transfer
