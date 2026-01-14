# setup.py for texera-r-plugin
# This package provides R language support for Apache Texera

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="texera-r-plugin",
    version="0.1.0",
    author="Texera Team",
    author_email="texera-dev@googlegroups.com",
    description="R language support plugin for Apache Texera",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Texera/texera-r-plugin",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "rpy2==3.5.11",
        "rpy2-arrow==0.0.8",
        "pyarrow>=21.0.0",
        "pandas>=2.2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-timeout>=2.2.0",
        ],
    },
    license="GPLv2",
    keywords="texera r rpy2 data-processing workflow",
)
