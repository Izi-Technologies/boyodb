"""Setup script for boyodb Python driver."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="boyodb",
    version="0.1.0",
    author="",
    author_email="",
    description="Python driver for boyodb-server - a high-performance columnar database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/loreste/boyodb",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
    ],
    python_requires=">=3.8",
    install_requires=[],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-asyncio>=0.20",
            "black>=23.0",
            "mypy>=1.0",
        ],
        "arrow": [
            "pyarrow>=12.0",
        ],
    },
)
