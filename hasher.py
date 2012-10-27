import os
import md5
import sys

BLOCKSIZE = 1024 * 1024
DEFAULT_FILENAME = ".hashes"
SCRIPT_FILENAME = "hasher_script.sh"
repeated = {}


def hexify(s):
    return ("%02x" * len(s)) % tuple(map(ord, s))


def getMD5(s):
    md5sum = md5.new(s)

    return hexify(md5sum.digest())


def generate_hashes(hash_file, update=False, append=False):
    cache = {}
    if (append or update) and os.path.exists(hash_file):
        load_hashfile(hash_file, cache_data=cache)

    new_hash_file = hash_file + ".new"

    if update:
        # Create a new file with only needed hashes
        outfile = asserted_open(new_hash_file, "w")
    else:
        if append:
            # Only appends new hashes
            outfile = asserted_open(hash_file, "a")
        else:
            # Create a new file
            outfile = asserted_open(hash_file, "w")

    for subdir, dirs, files in os.walk("."):
        if not files:
            continue
        sys.stdout.write(subdir + " -> ")
        if subdir == ".uma":
            sys.stdout.write("Ignored")
            continue
        if dirs.count(".uma") > 0:
            dirs.remove(".uma")
        for filename in files:
            if subdir == "." and (filename == hash_file or
                                  filename == new_hash_file):
                continue
            full_filename = subdir + "/" + filename
            file_stat = os.stat(full_filename)
            file_size = file_stat.st_size
            file_inode = file_stat.st_ino

            key = (full_filename +
                   str(file_stat.st_size) +
                   str(file_stat.st_mtime))
            md5key = getMD5(key)

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
                md5sum = md5.new()
                readcount = 0
                dirty = False
                while 1:
                    block = f.read(BLOCKSIZE)
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
                f.close()
                output = "%s|%s|%s|%s|%d|%d" % (md5key,
                                                hexify(md5sum.digest()),
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
        os.rename(new_hash_file, hash_file)
        if len(cache) > 0:
            print "%d old cache entries cleaned." % len(cache)
            for key in cache:
                cache_data = cache[key]
                print key + " -> " + cache_data[1] + "/" + cache_data[2]


def load_hashfile(filename, destDict=None, cache_data=None):
    f = asserted_open(filename, "r")

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
            if md5sum in repeated:
                repeated[md5sum].append(fileinfo)
            else:
                repeated[md5sum] = []
                repeated[md5sum].append(fileinfo)
                repeated[md5sum].append(destDict[md5sum])

        destDict[md5sum] = fileinfo

        if cache_data is not None:
            cache_data[key] = (md5sum, dirname, filename,
                               file_size, file_inode)

    f.close()

    return destDict


def compare(md5file1, md5file2):
    output_file_list = []

    md5a = load_hashfile(md5file1)
    md5b = load_hashfile(md5file2)

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
            print "Different: ", fdata1, fdata2
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

    if onlyIn1:
        print "\n# Those files only exist in", md5file1
        for i in onlyIn1:
            print i, onlyIn1[i]
    if md5b:
        print "\n# Those files only exist in", md5file2
        for i in md5b:
            print i, md5b[i]
    if commands:
        tee(output_file_list, "\n# Commands to execute in console:")

        mkdirs_list = list(mkdirs)
        rmdirs_list = list(rmdirs)

        mkdirs_list.sort()
        rmdirs_list.sort()
        rmdirs_list.reverse()
        commands.sort()

        tee(output_file_list, "\n# mkdir statements.\n")
        for i in mkdirs_list:
            tee(output_file_list, i)

        tee(output_file_list, "\n# mv statements.\n")
        for i in commands:
            tee(output_file_list, i)

        tee(output_file_list, "\n# Directories possibly empty from now. " +
                              "Please check.\n")
        for i in rmdirs_list:
            tee(output_file_list, "#" + i)

    if repeated:
        tee(output_file_list, """
# Those files are repeated. You can remove one or many of them if you wish:")
# (Note: if the inode is the same, there is no space wasted)
""")
        for key in repeated:

            tee(output_file_list, "\n# Key: " + key + " - Size: " +
                repeated[key][0][2])

            for repeated_file in repeated[key]:
                tee(output_file_list, "# rm '%s/%s' # inode: %s" % (
                    repeated_file[0],
                    repeated_file[1],
                    repeated_file[3]))

    if output_file_list != []:
        output_file_list[0].close()


def tee(output_file_list, s):
    if output_file_list == []:
        output_file = open(DEFAULT_FILENAME, "w")
        output_file_list.append(output_file)
    else:
        output_file = output_file_list[0]

    output_file.write(s + "\n")
    print s


def show_usage(appName):
    print """
Usage:

  Default filename: %defaultfilename%

  %appname% -g {filename} generate hashes (remove hashfile if exists)
  %appname% -a {filename} append hashes to file
  %appname% -u {filename} update hashfile (clean old entries, and append new)
  %appname% -c [filename1] [filename2] compares hashes from hashfiles

""".replace(
        "%appname%", appName
    ).replace(
        "%defaultfilename%", DEFAULT_FILENAME
    )


def asserted_open(filename, mode):
    f = None
    try:
        f = open(filename, mode)
    except IOError, e:
        print "I/O Error:", e

        sys.exit(2)
    except Exception, e:
        print "Error:", e
        raise e

    return f


if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_usage(sys.argv[0])
        sys.exit(1)

    if len(sys.argv) > 2:
        filename1 = sys.argv[2]

    if sys.argv[1].lower() == "-g":
        generate_hashes(filename1)
    elif sys.argv[1].lower() == "-a":
        generate_hashes(filename1, append=True)
    elif sys.argv[1].lower() == "-u":
        generate_hashes(filename1, update=True)
    elif sys.argv[1].lower() == "-c":
        if len(sys.argv) < 4:
            print "Error. Not enough parameters."
            show_usage(sys.argv[0])
            sys.exit(1)
        filename2 = sys.argv[3]
        compare(filename1, filename2)

    sys.exit(0)
