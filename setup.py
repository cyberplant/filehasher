from setuptools import setup

setup(name='filehasher',
      version='1.0.0',
      description='Modern file hashing utility with parallel processing, multiple algorithms, and benchmarking',
      url='http://github.com/cyberplant/filehasher',
      author='Luar Roji',
      author_email='cyberplant@roji.net',
      license='MIT',
      entry_points={
          'console_scripts': [
              'filehasher = filehasher.cmdline:main',
          ]},
      packages=['filehasher'],
      install_requires=[
          'tqdm>=4.0.0',
          'rich>=10.0.0',
      ],
      keywords=[
          'filehasher', 'file', 'hasher', 'hash', 'benchmark', 'dupe', 'duplicate'
      ],
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Programming Language :: Python :: 3.12',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Filesystems',
          'Topic :: Utilities',
          'Topic :: System :: Archiving',
          'License :: OSI Approved :: MIT License',
      ],
      zip_safe=True)
