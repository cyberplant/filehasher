from setuptools import setup

setup(name='filehasher',
      version='0.7',
      description='File Hasher with multiple algorithms and benchmarking',
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
          'Development Status :: 4 - Beta',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Filesystems',
          'Topic :: Utilities',
      ],
      zip_safe=True)
