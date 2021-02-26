from setuptools import find_packages, setup


setup(
    name='simeon',
    version='0.0.1',
    author='MIT Institutional Research',
    author_email='irx@mit.edu',
    packages=find_packages(),
    url='https://github.com/MIT-IR/simeon',
    license='MIT LICENSE',
    keywords=[
        'edx research data', 'mitx', 'edx',
        'MOOC', 'education', 'online learning'
    ],
    python_requires='>=3.6',
    description='Process research data from edX',
    long_description=open('README.rst').read(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'simeon=simeon.scripts.simeon:main',
            'simeon-geoip=simeon.scripts.geoip:main',
        ],
    },
    install_requires=[
        'boto3>=1.16.57',
        'google-cloud-bigquery>=2.6.2',
        'google-cloud-storage>=1.35.0',
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
        'Intended Audience :: Data Analysts :: Data Engineers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Data Analysis',
    ],
)
