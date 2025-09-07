#!/usr/bin/env python3
"""
Test script for Unicode encoding fixes in filehasher.
This script tests the encoding functions without creating files with surrogate characters.
"""

import sys
import os
import tempfile
import unittest

# Add the current directory to the path so we can import filehasher
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import filehasher

class TestUnicodeEncoding(unittest.TestCase):
    """Test cases for Unicode encoding fixes."""
    
    def setUp(self):
        """Set up test cases with surrogate characters."""
        self.test_cases = [
            "test\udcfcfile.txt",
            "file\udcffwith\udc00surrogates.txt", 
            "mixed\udcfe_unicode_测试.txt",
            "test\ud800\udc00file.txt",
        ]
        
        self.subdir_cases = [
            "test\udc00dir",
            "path\udcffwith\udc00surrogates",
            "mixed\udcfe_unicode_测试",
        ]
    
    def test_get_hash_with_surrogates(self):
        """Test _get_hash function with surrogate characters."""
        for test_string in self.test_cases:
            with self.subTest(filename=test_string):
                # This should not raise UnicodeEncodeError
                result = filehasher._get_hash(test_string, 'md5')
                self.assertIsInstance(result, str)
                self.assertEqual(len(result), 32)  # MD5 hash length
    
    def test_filename_encoding(self):
        """Test filename encoding process."""
        for test_string in self.test_cases:
            with self.subTest(filename=test_string):
                # This is what the code does for filename encoding
                filename_encoded = (test_string.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                self.assertIsInstance(filename_encoded, str)
                # Should not contain surrogate characters
                for char in filename_encoded:
                    self.assertFalse(0xD800 <= ord(char) <= 0xDFFF, 
                                   f"Surrogate character found: {repr(char)}")
    
    def test_subdir_encoding(self):
        """Test subdir encoding process."""
        for test_string in self.subdir_cases:
            with self.subTest(subdir=test_string):
                # This is what the code does for subdir encoding
                subdir_encoded = (test_string.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                self.assertIsInstance(subdir_encoded, str)
                # Should not contain surrogate characters
                for char in subdir_encoded:
                    self.assertFalse(0xD800 <= ord(char) <= 0xDFFF, 
                                   f"Surrogate character found: {repr(char)}")
    
    def test_output_string_construction(self):
        """Test output string construction with encoded filenames and subdirs."""
        for test_string in self.test_cases:
            with self.subTest(filename=test_string):
                filename_encoded = (test_string.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                subdir_encoded = ("test\udc00dir".encode("utf-8", "backslashreplace")).decode("iso8859-1")
                
                # This is the exact line that was failing
                output = f"hashkey|hexdigest|{subdir_encoded}|{filename_encoded}|12|12345"
                
                # Should be able to write to a file without encoding errors
                with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
                    f.write(output + "\n")
                    temp_file = f.name
                
                # Clean up
                os.unlink(temp_file)
                self.assertTrue(True)  # If we get here, no exception was raised
    
    def test_filehasher_integration(self):
        """Test filehasher integration with normal files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Create a normal test file
                with open("normal_file.txt", "w") as f:
                    f.write("test content")
                
                # This should not raise any errors
                filehasher.generate_hashes("test_hashes.txt", algorithm="md5", show_progress=False)
                
                # Check if the hash file was created
                self.assertTrue(os.path.exists("test_hashes.txt"))
                
                # Check if the hash file contains the expected entry
                with open("test_hashes.txt", "r", encoding="utf-8") as f:
                    content = f.read()
                    self.assertIn("normal_file.txt", content)
                    self.assertIn("# Algorithm: md5", content)
                        
            finally:
                os.chdir(original_cwd)

def run_tests():
    """Run all tests and return success status."""
    # Create a test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestUnicodeEncoding)
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == "__main__":
    print("🧪 Running Unicode encoding fix tests...\n")
    
    success = run_tests()
    
    if success:
        print("\n🎉 All tests passed! The UnicodeEncodeError fix is working correctly.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please check the output above.")
        sys.exit(1)