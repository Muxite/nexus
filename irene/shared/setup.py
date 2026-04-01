from setuptools import setup, find_packages

setup(
    name="irene_shared",
    version="0.1.0",
    packages=find_packages(),
    package_data={"irene_shared": ["migrations/*.sql"]},
    install_requires=[
        "pydantic>=2.0",
        "pyyaml>=6.0",
        "asyncpg>=0.29",
        "redis>=5.0",
        "numpy>=1.24",
    ],
)
