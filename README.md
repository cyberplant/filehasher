# FileHasher

> A modern, high-performance file hashing utility with parallel processing, multiple algorithms, and beautiful progress bars.

FileHasher generates cryptographic hashes for files in directories, allowing you to:
- **Compare directory contents** across different machines or backups
- **Find duplicate files** within or across directories
- **Detect file changes** and corruption
- **Synchronize directories** with automated scripts

## ✨ Features

### 🚀 **High Performance**
- **Parallel processing** with multiple worker threads
- **Individual progress bars** for each worker
- **Multi-core optimization** for modern CPUs
- **Real-time progress** with Rich terminal UI

### 🔐 **Multiple Hash Algorithms**
- **MD5** (default, backward compatible)
- **SHA1**, **SHA256**, **SHA512**
- **Blake2B**, **Blake2S**
- **Algorithm tracking** in hash files
- **Mismatch warnings** when switching algorithms

### 📊 **Benchmarking & Analysis**
- **Performance comparison** of all algorithms
- **Throughput measurements** (MB/s)
- **Algorithm recommendations** based on your hardware

### 🎯 **Modern CLI**
- **Rich progress bars** with colors and animations
- **Verbose output** showing current files being processed
- **Configuration file** support (`.filehasher.ini`)
- **Comprehensive help** and examples

## 📦 Installation

```bash
pip install filehasher
```

**Requirements:**
- Python 3.6+
- tqdm (for fallback progress bars)
- rich (for enhanced UI)

## 🚀 Quick Start

### Basic Usage

1. **Generate hashes** for current directory:
```bash
filehasher --generate
```

2. **Compare directories**:
```bash
# Generate hashes for source
filehasher --generate .source-hashes

# Generate hashes for destination
filehasher --generate .dest-hashes

# Compare and generate sync script
filehasher --compare .dest-hashes .source-hashes
```

3. **Edit and run** the generated script:
```bash
# Edit filehasher_script.sh to customize actions
vim filehasher_script.sh

# Run the synchronization script
bash filehasher_script.sh
```

## 🎨 Advanced Features

### Parallel Processing with Individual Worker Progress

```bash
# Use 4 workers with individual progress bars
filehasher --generate --parallel --workers 4

# Verbose output showing current files
filehasher --generate --parallel --verbose --algorithm sha256
```

**Visual Output:**
```
Generating sha256 hashes...
  Worker 1 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% (153/153) current_file.jpg
  Worker 2 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% (153/153) another_file.txt
  Worker 3 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% (153/153) processing_file.dat
  Worker 4 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% (152/152) final_file.py
```

### Algorithm Selection & Benchmarking

```bash
# Use specific algorithm
filehasher --generate --algorithm sha256

# Benchmark all algorithms on your hardware
filehasher --benchmark

# Benchmark with custom test file
filehasher --benchmark --benchmark-file large_video.mp4
```

**Benchmark Results:**
```
Benchmark Results:
--------------------------------------------------------------------------------
Algorithm     Avg Time (s)    Min Time (s)    Max Time (s)    Throughput (MB/s)
--------------------------------------------------------------------------------
sha256             0.0045       0.0044       0.0047          2199.27
sha1               0.0047       0.0047       0.0048          2106.49
sha512             0.0075       0.0075       0.0076          1329.89
blake2b            0.0146       0.0146       0.0147           682.83
md5                0.0160       0.0153       0.0163           626.82
blake2s            0.0247       0.0246       0.0248           405.60
```

### Configuration File

Create `.filehasher.ini` in your project directory:

```ini
[filehasher]
default_algorithm = sha256
benchmark_iterations = 5
quiet = false
```

## 📋 Complete Usage Guide

### Command Line Options

```
Usage: filehasher [OPTIONS] [HASHFILE]

File Hasher - Generate and compare file hashes with multiple algorithms.

Options:
  -g, --generate           Generate hashes (remove hashfile if exists)
  -a, --append            Append hashes to hashfile
  -u, --update            Update hashfile (clean old entries and append new)
  -A, --algorithm         Hash algorithm (default: md5)
  -b, --benchmark         Benchmark all supported hash algorithms
  -P, --parallel          Process files in parallel using multiple workers
  --workers WORKERS       Number of parallel workers (default: CPU count)
  -v, --verbose           Show detailed progress including filenames
  -q, --quiet             Suppress progress output
  -c, --compare           Compare hashes from hashfiles
  -h, --help              Show this help message

Supported algorithms: md5, sha1, sha256, sha512, blake2b, blake2s
```

### Directory Synchronization Example

1. **Setup source directory**:
```bash
cd /path/to/source
filehasher --generate --algorithm sha256 .source-hashes
```

2. **Setup destination directory**:
```bash
cd /path/to/destination
filehasher --generate --algorithm sha256 .dest-hashes
```

3. **Compare and generate sync script**:
```bash
filehasher --compare .dest-hashes .source-hashes
```

4. **Review and execute**:
```bash
# Check what will be done
cat filehasher_script.sh

# Edit if needed
vim filehasher_script.sh

# Execute synchronization
bash filehasher_script.sh
```

### Duplicate File Detection

```bash
# Generate/update hashes
filehasher --update

# Find duplicates within directory
filehasher --compare .hashes .hashes
```

The generated script will show:
- Files with identical content (duplicates)
- Commands to remove unwanted duplicates

## 🔧 Algorithm Selection Guide

| Algorithm | Security | Speed | Use Case |
|-----------|----------|-------|----------|
| **MD5** | ⚠️ Weak | 🚀 Fastest | Legacy compatibility |
| **SHA1** | ⚠️ Deprecated | ⚡ Fast | Quick integrity checks |
| **SHA256** | ✅ Strong | ⚡ Fast | General purpose, recommended |
| **SHA512** | ✅ Very Strong | 🐌 Slower | High-security requirements |
| **Blake2B** | ✅ Very Strong | ⚡ Fast | Modern, high-performance |
| **Blake2S** | ✅ Strong | ⚡ Fast | 32-bit systems, embedded |

## 🚨 Important Notes

### Algorithm Mismatch Protection
When switching algorithms, FileHasher warns you:
```
⚠️  WARNING: Algorithm mismatch detected!
   Existing hash file uses: md5
   Requested algorithm: sha256
   This may cause inconsistent hash comparisons.
   Continue anyway? (y/N):
```

### Parallel Processing Benefits
- **Multi-core CPUs**: Near-linear speedup with worker count
- **Large directories**: Significantly faster processing
- **Progress visibility**: Individual worker progress tracking
- **Resource efficient**: Optimal load balancing

### Configuration
Default settings can be customized in `.filehasher.ini`:
```ini
[filehasher]
default_algorithm = sha256
benchmark_iterations = 3
quiet = false
```

## 🤝 Contributing

FileHasher is open source and welcomes contributions!

## 📄 License

MIT License - see LICENSE file for details.

---

**FileHasher** - Modern file integrity and synchronization made beautiful.
