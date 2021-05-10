import os
import setuptools

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setuptools.setup(
    name = "cloud-pricing",
    version = "0.0.1",
    author = "Thomas Tumiel",
    description = ("Compare cloud compute prices."),
    license = "MIT",
    keywords = "cloud",
    packages=setuptools.find_packages(),
    long_description=read('README.md'),
    entry_points = {
        'console_scripts': ['cloud-pricing=cloud_pricing.main:main'],
    },
    install_requires=[
        'numpy',
        'pandas',
        'requests',
        'beautifulsoup4',
        'tqdm',
        'lxml'
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
    ],
)
