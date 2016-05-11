from setuptools import setup

setup(name='filehasher',
      version='0.3',
      description='File Hasher',
      url='http://github.com/cyberplant/filehasher',
      author='Luar Roji',
      author_email='cyberplant@roji.net',
      license='MIT',
      entry_points={
          'console_scripts': [
              'filehasher = filehasher.cmdline:main',
          ]},
      packages=['filehasher'],
      keywords=[
          'filehasher', 'file', 'hasher', 'dupe'
      ],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      zip_safe=True)
