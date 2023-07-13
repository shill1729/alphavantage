from setuptools import setup

setup(name='alphavantage',
      version='0.1.4',
      description='A python wrapper to the Alpha Vantage API for financial data',
      url='https://github.com/shill1729/alphavantage',
      author='S. Hill',
      author_email='52792611+shill1729@users.noreply.github.com',
      license='MIT',
      packages=['alphavantage'],
      install_requires=[
          'np',
          'pandas',
          "requests",
      ],
      zip_safe=False)

