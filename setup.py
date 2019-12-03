from setuptools import setup, find_packages

version = "0.3.0"

long_description = open("README.rst").read() + "\n" + open("CHANGES.rst").read() + "\n"

setup(
    name="imio.amqp",
    version=version,
    description="",
    long_description=long_description,
    # Get more strings from
    # http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=["Programming Language :: Python"],
    keywords="",
    author="IMIO",
    author_email="support@imio.be",
    url="https://github.com/imio/",
    license="GPL",
    packages=find_packages("src"),
    package_dir={"": "src"},
    namespace_packages=["imio"],
    include_package_data=True,
    zip_safe=False,
    install_requires=["setuptools", "pika"],
    extras_require=dict(test=["mock"]),
    entry_points="""
    # -*- Entry points: -*-
    """,
)
