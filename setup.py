import setuptools

with open("README.md") as readme_file:
    readme = readme_file.read()

with open("requirements.txt") as requirements_file:
    requirements = requirements_file.read().splitlines()

with open("opt_requirements.txt") as requirements_file:
    tests_requirements = requirements_file.read().splitlines()

setuptools.setup(
    name="hero_db_utils",
    version="1.3.1",
    url="https://github.com/AIO2020/hero-db-utils",
    description="Database utilities for projects related to the AIO-hero platform.",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Jose Sanchez Viloria",
    author_email="jose.sanchez@aio.ai",
    packages=setuptools.find_packages(exclude=["tests.*", "tests"]),
    python_requires=">=3.6",
    install_requires=requirements,
    test_suite="tests",
    tests_require=tests_requirements,
)
