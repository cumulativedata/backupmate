from setuptools import setup, find_packages

setup(
    name="backupmate",
    version="0.1.0",
    description="A CLI tool for automating MariaDB backups using Mariabackup and AWS S3",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        'boto3>=1.26.0',  # For AWS S3 interactions
        'python-dotenv>=0.19.0',  # For loading .env files
    ],
    entry_points={
        'console_scripts': [
            'backupmate=backupmate.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: Database',
    ],
    python_requires='>=3.7',
)
