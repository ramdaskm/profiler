import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dbr-profiler-tool",
    version="0.2.6",
    author="RKMurali",
    author_email="ramdas.murali@databricks.com",
    description="Databricks Profiling Tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/yoohoo",
    license="https://github.com/databrickslabs/profile/blob/master/LICENSE",
    packages=["clientpkgs", "core"],
    install_requires=[
          'requests'
      ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6'
)
