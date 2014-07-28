Preliminary
-----------

I have a large multimedia collection of pictures/videos of my family, travel, etc.

I also have those files in a hosting, published with a media album (that I'm developing, also).

The need for **_filehasher_** started when I renamed a set of directories and files, and have to 
synchronize the changes with the hosting. My upload bandwidth is very limited, so I cannot 
afford to upload 100Gb of files again (it would take like two months 24hs!).

So, if you have a large set of files and want to synchronize some changes in directories or
filenames, filehasher is the right tool for you.

I use rsync for the file copy, so it would be a good companion for it.

First Time Usage
----------------

1. Download **_filehasher_** to your two (or more) machines. The _source_ and _destination_.

2. Generate hashes in the two collections, so the program know which files are 
   the same than others.
```
   [user@source] % filehasher -g .orig-hashes
   [user@destination] % filehasher -g .dest-hashes
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
   are missing from one list of files, etc. If you miss something, don't worry,
   the program has created a file named **filehasher_script.sh** that you have to 
   edit and run.

5. Edit the file **filehasher_script.sh**

   At the beginning are the mkdir's needed. After that the moves needed to make
   the _destination_ file set equal to _source_ file set.

   After that you will see rmdirs of directories that now can be empty. Maybe 
   they are not empty, you will have to check manually, or uncomment them, if
   they are not empty anyways they will don't be removed (rmdir will raise an
   error).

   After the rmdirs you can see a lot of "rm"s commented. Now you will have to
   use your text editing skills. This is a list of duplicated files. You have
   to uncomment the line of the rm of the file that you don't want to keep.

6. Run the modified script file

   After this you will have a very similar directory in _destination_ compared
   to _source_. In my own experience, it's better to run a second time so you
   can check any smaller differences with more time.

Common Usage
------------

1. Update the hashes in the two collections
```
   user@source % filehasher -u .orig-hashes

   user@destination % filehasher -u .dest-hashes
```
2. See steps 3 to 6 from "First time Usage" 


Credits/Thanks
--------------

- When I started this project there were a lot of things that I didn't know
  how to do, so I want to thanks Internet and Google for helping me out :)

- @CMAD, that made me realize what I really need! Thanks!

- My great girlfriend @roschegel, she helped me testing it. I love you Ro! :)

- prh for giving me hints to improve my code.
