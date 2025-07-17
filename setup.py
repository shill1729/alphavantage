from setuptools import setup, find_packages


def parse_requirements(filename):
    with open(filename, 'r') as f:
        return f.read().splitlines()

setup(name='alphavantage',
      version='0.1.7',
      description='A python wrapper to the Alpha Vantage API for financial data',
      url='https://github.com/shill1729/alphavantage',
      author='S. Hill',
      author_email='52792611+shill1729@users.noreply.github.com',
      license='MIT',
      packages=find_packages(include=["alphavantage", "alphavantage.*"]),
      install_requires=parse_requirements("requirements.txt"),
      zip_safe=False)
