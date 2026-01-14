# Texera R Plugin

R language support for [Apache Texera](https://github.com/apache/texera), enabling data processing workflows using R code.

## Installation

### Prerequisites

**R Installation** (version 4.5.2)
- Download from: https://www.r-project.org/
- Other R versions may work but have not been tested

**Required R packages** (install these specific versions):
```r
# Install tested versions
install.packages("remotes")
remotes::install_version("arrow", version = "22.0.0.1")
remotes::install_version("coro", version = "1.1.0")
remotes::install_version("aws.s3", version = "0.3.22")
```

### Install Plugin

```bash
# Install from GitHub
pip install git+https://github.com/kunwp1/texera-r-plugin.git

# Development install
git clone https://github.com/kunwp1/texera-r-plugin.git
cd texera-r-plugin
pip install -e .
```

## Usage

The plugin provides two APIs for processing data in Texera workflows:

### Tuple API (Row-by-Row Processing)

**Source Operator:**
```r
library(coro)
coro::generator(function() {
  yield(list(col1 = "Hello World!", col2 = 1.0, col3 = TRUE))
})
```

**UDF Operator:**
```r
library(coro)
coro::generator(function(tuple, port) {
  tuple$col4 <- tuple$col2 * 2
  yield(tuple)
})
```

### Table API (Batch Processing)

**Source Operator:**
```r
function() {
  df <- data.frame(
    col1 = "Hello World!",
    col2 = 1.0,
    col3 = TRUE
  )
  return(df)
}
```

**UDF Operator:**
```r
function(table, port) {
  table$col4 <- table$col2 * 2
  return(table)
}
```

### Large Binary Support

Handle large binary objects (images, files, etc.) via S3-compatible storage:

**Writing Large Binary:**
```r
library(coro)
coro::generator(function() {
  # Create a new large binary object
  lb <- largebinary()
  
  # Write data to it
  stream <- LargeBinaryOutputStream(lb)
  stream$write(charToRaw("Hello, Large Binary World!"))
  stream$close()
  
  yield(list(file_content = lb))
})
```

**Reading Large Binary:**
```r
library(coro)
coro::generator(function(tuple, port) {
  # Read from large binary object
  stream <- LargeBinaryInputStream(tuple$file_content)
  data <- stream$read()
  stream$close()
  
  # Convert raw bytes to string
  content <- rawToChar(data)
  
  tuple$content_text <- content
  yield(tuple)
})
```

## Features

- **Tuple API**: Row-by-row processing with R generators
- **Table API**: Batch processing with R dataframes
- **Apache Arrow**: Efficient data transfer between Python and R
- **Large Binary Support**: Handle large objects via S3-compatible storage

## Requirements

### Tested Versions

This plugin has been tested and verified to work with the following versions:

**Python Environment:**
- **Python**: 3.10, 3.11, 3.12
- **rpy2**: 3.5.11
- **rpy2-arrow**: 0.0.8

**R Environment:**
- **R**: 4.5.2
- **arrow**: 22.0.0.1
- **coro**: 1.1.0
- **aws.s3**: 0.3.22

Other versions may work but have not been tested and are not guaranteed to be compatible.

## License

Licensed under the **GNU General Public License v2.0** (due to rpy2 dependency). See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Links

- **Issues**: https://github.com/kunwp1/texera-r-plugin/issues
- **Apache Texera**: https://github.com/apache/texera
- **rpy2**: https://github.com/rpy2/rpy2
