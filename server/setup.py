from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
setup(
    name="okx-binance-bot",
    version="0.0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "okx-binance-bot = utils.cli:cli",
        ]
    },
    author="okx-binance-bot",
    author_email="abel@example.com",
    description="Welcome to okx-binance-bot",
    long_description=open("readme.rst").read(),
    long_description_content_type="text/x-rst",
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    include_package_data=True,
    install_requires=requirements,
    extras_require={"dev": ["pytest", "wheel", "twine", "black", "setuptools"]},
    dependency_links=[
        "git+ssh://git@github.com/infinity-sandbox/okx-binance-bot.git@76dd10709e9a11bf66ca4949f9f31d8daaa4f98a#egg=okx_binance_bot&subdirectory=server"
    ]
)