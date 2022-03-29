import setuptools


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('README.md', 'r') as fh:
    long_description = fh.read()


setuptools.setup(
    name = 'hdfs_manager',
    version = '0.1.0',
    author = 'dcarve',
    author_email = 'daniel.ermelino.carvalho@gmail.com',
    description = '',
    long_description  = long_description,
    long_description_content_type = 'text/manager',
    url = 'https://github.com/dcarve/hdfs_manager',
    packages=setuptools.find_packages(),
    install_requires = requirements,
    include_package_data = True,
    classifiers = [
       "Programming Language :: Python :: 3",
       "Operating System :: POSIX :: Linux"
    ]
)


