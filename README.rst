FileHasher
==========

The purpose of this tool is to allow you to get a list of hashes of the
files of some directories, compare those lists and generate a script
with commands to equalize those directories.

For instance, if you have two directories that were the same but on one
of them you renamed files, or even moved some to other directories, with
this tool you can get a script that allows you to make the second directory
look again like the first one.


First Time Usage
================

1. Install "filehasher" to your two (or more) machines. 
   The 'source' and 'destination'.

   % pip install filehasher


2. Generate hashes in the two collections

   [user@source /directory] % filehasher -g .orig-hashes
   [user@destination /directory] % filehasher -g .dest-hashes


3. Copy the orig-hashes to _destination_ machine.

    [user@source] % scp .orig-hashes user@destination:directory


4. Compare the hashes

    [user@destination] % filehasher -c .dest-hashes .orig-hashes

This will tell you a lot of things, which files have changed, which files
are missing from one list of files, etc. and will create a script called
"filehasher_script.sh" that you have to edit and run.


5. Edit the file "filehasher_script.sh"

At the beginning are the mkdir's needed. After that the mv's needed to make
the 'destination' file set equal to 'source' file set.

After that you will see rmdir's of directories that now can be empty. Maybe 
they are not empty, you will have to check manually, or uncomment them, if
they are not empty anyways they will don't be removed (rmdir will raise an
error).

After the rmdir's you can see a lot of rm's commented. Now you will have to
use your text editing skills. This is a list of duplicated files. You have
to uncomment the line of the rm of the file that you don't want to keep.


6. Run the modified script file

After this you will have a very similar directory in 'destination' compared
to 'source'. In my own experience, it's better to run a second time so you
can check any smaller differences with more time.

Use FileHasher as a dupe finder
===============================

I've found accidentally that you can generate the hashes and run the check
against the same file to found which files are duplicated on your directory.

1. Update the hashes file

   % filehasher -u 

   % filehasher -c .hashes .hashes

2. Check the file filehasher_script.sh to see the repeated files and 
   uncomment the remove statements to clean your directories.
