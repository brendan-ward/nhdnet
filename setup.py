from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="nhdnet",
    version="0.2.0",
    description="US National Hydrography Dataset Network and Barrier Analysis Tools",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/brendan-ward/nhdnet",
    author="Brendan C. Ward",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
    ],
    keywords="nhd hydrography",
    packages=find_packages(exclude=["docs", "tests"]),
    install_requires=["pandas", "geopandas", "rtree", "geofeather", "requests"],
    extras_require={"dev": ["black", "pylint"], "test": ["pytest", "pytest-cov"]},
)
