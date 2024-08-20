from setuptools import setup

version = {}
with open("filehasher/version.py") as fp:
    exec(fp.read(), version)

setup(name='filehasher',
      version=version["__version__"],
      description='File Hasher',
      url='http://github.com/cyberplant/filehasher',
      author='Luar Roji',
      author_email='luar@roji.net',
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
