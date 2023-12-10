#!/usr/bin/env python

import os
import hashlib
import sys


class FileHasher():
    BLOCKSIZE = 1024 * 1024
    SCRIPT_FILENAME = "filehasher_script.sh"
    repeated = {}
    hash_file = None

    def __init__(self, hash_file):
        self.hash_file = hash_file

    def _getMD5(self, s):
        return hashlib.md5(s.encode("utf-8", "surrogateescape")).hexdigest()

    def calculate_md5(self, f):
        md5sum = hashlib.md5()
        readcount = 0
        dirty = False
        while 1:
            block = f.read(self.BLOCKSIZE)
            if not block:
                break
            readcount = readcount + 1
            if readcount > 10 and readcount % 23 == 0:
                if dirty:
                    sys.stdout.write("\b")
                dirty = True
                sys.stdout.write(("/", "|", '\\', "-")[readcount % 4])
                sys.stdout.flush()
            md5sum.update(block)

        return md5sum, dirty


    def generate_hashes(self, update=False, append=False):
        cache = {}
        if (append or update) and os.path.exists(self.hash_file):
            self._load_hashfile(self.hash_file, cache_data=cache)

        new_hash_file = self.hash_file + ".new"

        files_to_ignore = set(self.hash_file)

        if update:
            # Create a new file with only needed hashes
            outfile = self._asserted_open(new_hash_file, "w")
        else:
            if append:
                # Only appends new hashes
                outfile = self._asserted_open(self.hash_file, "a")
            else:
                # Create a new file
                outfile = self._asserted_open(self.hash_file, "w")

        for subdir, dirs, files in os.walk("."):
            if not files:
                continue
            sys.stdout.write(subdir + " -> ")
            if subdir in files_to_ignore:
                if dirs.count(subdir) > 0:
                    dirs.remove(subdir)
                sys.stdout.write("Ignored")
                continue

            for filename in files:
                if subdir == "." and filename in files_to_ignore:
                    continue
                full_filename = subdir + "/" + filename

                if os.path.islink(full_filename):
                    # Don't process symlinks
                    continue

                file_stat = os.stat(full_filename)
                file_size = file_stat.st_size
                file_inode = file_stat.st_ino

                key = (full_filename +
                       str(file_stat.st_size) +
                       str(file_stat.st_mtime))
                md5key = self._getMD5(key)
                filename = (filename.encode("utf-8", "backslashreplace")).decode("utf-8")

                if md5key in cache:
                    sys.stdout.write("C")
                    if update:
                        cache_data = cache.pop(md5key)
                        output = "%s|%s|%s|%s|%s|%s" % (md5key, cache_data[0],
                                                        subdir, filename,
                                                        cache_data[3],
                                                        cache_data[4])
                        outfile.write(output + "\n")
                else:

                    f = open(full_filename, "rb")
                    try:
                        md5sum, dirty = self.calculate_md5(f)
                    except Exception as e:
                        print("Error:", e)
                        continue
                    f.close()
                    output = "%s|%s|%s|%s|%d|%d" % (md5key,
                                                    md5sum.hexdigest(),
                                                    subdir, filename, file_size,
                                                    file_inode)
                    outfile.write(output + "\n")
                    if dirty:
                        sys.stdout.write("\b")
                    sys.stdout.write(".")
                sys.stdout.flush()
            sys.stdout.write("\n")
        outfile.close()
        if update:
            os.rename(new_hash_file, self.hash_file)
            if len(cache) > 0:
                print("%d old cache entries cleaned." % len(cache))
                for key in cache:
                    cache_data = cache[key]
                    print(key + " -> " + cache_data[1] + "/" + cache_data[2])


    def _load_hashfile(self, filename, destDict=None, cache_data=None):
        f = self._asserted_open(filename, "r")

        if destDict is None:
            destDict = {}

        while 1:
            line = f.readline()
            if not line:
                break
            line = line[0:len(line) - 1]
            key, md5sum, dirname, filename, file_size, file_inode = line.split("|")
            fileinfo = (dirname, filename, file_size, file_inode)

            if md5sum in destDict:
                if md5sum in self.repeated:
                    self.repeated[md5sum].append(fileinfo)
                else:
                    self.repeated[md5sum] = []
                    self.repeated[md5sum].append(fileinfo)
                    self.repeated[md5sum].append(destDict[md5sum])

            destDict[md5sum] = fileinfo

            if cache_data is not None:
                cache_data[key] = (md5sum, dirname, filename,
                                   file_size, file_inode)

        f.close()

        return destDict


    def compare(self, md5file_to_compare = None):
        global output_file

        output_file = None

        md5a = self._load_hashfile(self.hash_file)
        md5b = []
        if md5file_to_compare:
            md5b = self._load_hashfile(md5file_to_compare)

        commands = []
        mkdirs = set()
        rmdirs = set()
        onlyIn1 = {}
        i = 0
        for hashmd5 in md5a:
            fdata1 = md5a[hashmd5]
            fdata2 = None

            if hashmd5 in md5b:
                fdata2 = md5b[hashmd5]
                md5b.pop(hashmd5)
            else:
                onlyIn1[hashmd5] = fdata1
                continue

            if fdata2[0] == fdata1[0] and fdata2[1] == fdata1[1]:
                # The files are the same
                continue
            else:
                print("Different: ", fdata1, fdata2)
                if fdata1[0] == fdata2[0]:
                    # Same directory, only filename changed
                    commands.append("mv -v '%s/%s' '%s/%s'" % (fdata1[0],
                                                               fdata1[1],
                                                               fdata2[0],
                                                               fdata2[1]))
                else:
                    # Directory changed and possibly filename too
                    mkdirs.add("mkdir -pv '%s'" % fdata2[0])
                    if fdata1[0] != ".":
                        rmdirs.add("rmdir -v '%s'" % fdata1[0])

                    commands.append("mv -v '%s/%s' '%s/%s'" % (fdata1[0],
                                                               fdata1[1],
                                                               fdata2[0],
                                                               fdata2[1]))

        if md5file_to_compare:
            if onlyIn1:
                print("\n# Those files only exist in", self.hash_file)

                for item in _sorted_filenames(onlyIn1):
                    print(item)

            if md5b:
                print("\n# Those files only exist in", md5file_to_compare)

                for item in _sorted_filenames(md5b):
                    print(item)

        if commands:
            self.tee(output_file, "\n# Commands to execute in console:")

            mkdirs_list = list(mkdirs)
            rmdirs_list = list(rmdirs)

            mkdirs_list.sort()
            rmdirs_list.sort()
            rmdirs_list.reverse()
            commands.sort()

            self.tee(output_file, "\n# mkdir statements.\n")
            for i in mkdirs_list:
                self.tee(output_file, i)

            self.tee(output_file, "\n# mv statements.\n")
            for i in commands:
                self.tee(output_file, i)

            self.tee(output_file, "\n# Directories possibly empty from now. " +
                             "Please check.\n")
            for i in rmdirs_list:
                self.tee(output_file, "#" + i)

        if self.repeated:
            self.tee(output_file, """
# Those files are repeated. You can remove one or many of them if you wish:")
# (Note: if the inode is the same, there is no space wasted)
    """)
            filenames = []
            for item in self.repeated:
                filenames += [(l[0], l[1], item) for l in self.repeated[item]]

            filenames.sort()

            while len(filenames) > 0:
                item = filenames.pop(0)
                item_key = item[2]

                if item_key in self.repeated:
                    repeated_files = self.repeated.pop(item_key)
                    self.tee(output_file, "\n# Key: {key} - Size: {filesize}".format(
                        key=item_key,
                        filesize=repeated_files[0][2]
                    ))

                    c = set()

                    for repeated_file in repeated_files:
                        c.add("# rm '{dirname}/{fname}' # inode: {inode}".format(
                            dirname=repeated_file[0],
                            fname=repeated_file[1],
                            inode=repeated_file[3],
                        ))

                    commands = list(c)
                    commands.sort()
                    self.tee(output_file, "\n".join(commands))

        if output_file is not None:
            output_file.close()


    def _sorted_filenames(self, filelist):
        filenames = ["/".join((filelist[item][0], filelist[item][1]))
                     for item in filelist]
        filenames.sort()
        return filenames


    def tee(self, o, s):
        if o is None:
            o = open(self.SCRIPT_FILENAME, "w")

            global output_file
            output_file = o

        o.write(s + "\n")
        print(s)


    def _asserted_open(self, filename, mode):
        f = None
        try:
            f = open(filename, mode)
        except IOError as e:
            print("I/O Error:", e)

            sys.exit(2)
        except Exception as e:
            print("Error:", e)
            raise e

        return f
