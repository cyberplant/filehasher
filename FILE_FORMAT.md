

Example file format:

[file hash] | [other hash] | [directory] | [filename] | file_size | inode | last_modification_time

if ":" in [file hash] then:
  hashing_algorithm, hash = file_hash.split(":")

6dfa8d48aa3f342b94453d402603a055|3b39ce7494e76840a359a4132fef3077|.|filehasher_script.sh|4353353|370222|12345
1d0ebff3b596a170f60b53d86a50afcb|1968919a72e30de46cc64ebb613e10d2|.|.DS_Store|61444|375198|12345
MD5:801ea7f9fc47dcf0e6e06ae4e63be92b|d75612f3674ba56c8aa13e12d38b1b93|.|.oldhashes|66055955|567367|12345


