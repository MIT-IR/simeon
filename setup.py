import os
from setuptools import find_packages, setup

# Get the value of __version__ from the library's __init__.py file
exec(open(os.path.join('simeon', '__init__.py')).read())

setup(
    name='simeon',
    version=globals().get('__version__', '0.0.1'),
    author='MIT Institutional Research',
    author_email='irx@mit.edu',
    packages=find_packages(exclude=('docs',)),
    url='https://github.com/MIT-IR/simeon',
    license='MIT LICENSE',
    keywords=[
        'edx research data', 'mitx', 'edx',
        'MOOC', 'education', 'online learning'
    ],
    python_requires='>=3.6',
    description='A CLI tool to help process research data from edX',
    long_description=open('README.rst').read(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'simeon=simeon.scripts.simeon:main',
            'simeon-geoip=simeon.scripts.geoip:main',
            'simeon-youtube=simeon.scripts.youtube:main',
        ],
    },
    install_requires=[
        'boto3>=1.16.57',
        'google-cloud-bigquery>=2.6.2',
        'google-cloud-storage>=1.35.0',
        'jinja2',
        'python-dateutil>=2.8.1',
    ],
    extras_require={
        'geoip': ['geoip2'],
        'test': ['sphinx', 'tox'],
    },
    package_data={
        'simeon.upload': ['schemas/*.json'],
        'simeon.report': ['queries/*.sql'],
    },
    test_suite="simeon.tests",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Text Processing',
    ],
)
