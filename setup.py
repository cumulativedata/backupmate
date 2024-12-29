from setuptools import setup, find_packages

setup(
    name='backupmate',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'python-dotenv',
        'boto3',
        'argparse',
        'SQLAlchemy' # Add SQLAlchemy as a dependency
    ],
    entry_points={
        'console_scripts': [
            'backupmate=backupmate.cli:main',
        ],
    },
)
