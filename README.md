FileHasher
----------

The purpose of this tool is to allow you to get a list of hashes of the
files of some directories, compare those lists and generate a script
with commands to equalize those directories.

For instance, if you have two directories that were the same but on one
of them you renamed files, or even moved some to other directories, with
this tool you can get a script that allows you to make the second directory
look again like the first one.

First Time Usage
----------------

1. Install **_filehasher_** to your two (or more) machines. 
   The _source_ and _destination_.
```
   % pip install filehasher
```

2. Generate hashes in the two collections
```
   [user@source /directory] % filehasher -g .orig-hashes
   [user@destination /directory] % filehasher -g .dest-hashes
```

3. Copy the orig-hashes to _destination_ machine.
```
    [user@source] % scp .orig-hashes user@destination:directory
```

4. Compare the hashes
```
    [user@destination] % filehasher -c .dest-hashes .orig-hashes
```
   This will tell you a lot of things, which files have changed, which files
   are missing from one list of files, etc. and will create a script called
   **filehasher_script.sh** that you have to edit and run.


5. Edit the file **filehasher_script.sh**

   At the beginning are the mkdir's needed. After that the mv's needed to make
   the _destination_ file set equal to _source_ file set.

   After that you will see rmdir's of directories that now can be empty. Maybe 
   they are not empty, you will have to check manually, or uncomment them, if
   they are not empty anyways they will don't be removed (rmdir will raise an
   error).

   After the rmdir's you can see a lot of rm's commented. Now you will have to
   use your text editing skills. This is a list of duplicated files. You have
   to uncomment the line of the rm of the file that you don't want to keep.


6. Run the modified script file

   After this you will have a very similar directory in _destination_ compared
   to _source_. In my own experience, it's better to run a second time so you
   can check any smaller differences with more time.


Use FileHasher as a dupe finder
-------------------------------

I've found accidentally that you can generate the hashes and run the check
against the same file to found which files are duplicated on your directory.

1. Update the hashes file
```
   % filehasher -u 

   % filehasher -c .hashes .hashes
```

2. Check the file filehasher_script.sh to see the repeated files and 
   uncomment the remove statements to clean your directories.
   

History
-------

I have a large multimedia collection of pictures/videos of my family, 
travel, etc.

I also have those files in a hosting, published with a media album 
(that I'm developing, also).

The need for **_filehasher_** started when I started organizing my
media album and renamed a set of directories and files. Then realized
that those files were already uploaded and with the tools I had I will
need to upload all the files again to have my remote media album in sync!

As my upload bandwidth was very limited (512Kbps), the time I should
have spent to reupload those files was near two months 24hs!

With this tool, I synchronized my changes in less than 15 minutes.

For the file copying to remote location I use rsync, and I think is the
best tool, unless you rename files :)


Credits/Thanks
--------------

- When I started this project there were a lot of things that I didn't know
  how to do, so I want to thanks Internet and Google for helping me out :)

- @CMAD, that made me realize what I really need! Thanks!

- My great girlfriend @rocioar. I love you Ro! :)

- @pablohoffman for giving me hints to improve my code.
