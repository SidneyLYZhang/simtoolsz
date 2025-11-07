#!/usr/bin/env python3
"""Test script for the optimized getreader function."""

import sys
import traceback
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from simtoolsz.reader import getreader

def test_basic_functionality():
    """Test basic functionality of getreader."""
    print("Testing basic functionality...")
    
    # Test CSV reader
    reader = getreader("test.csv")
    assert reader.__name__ == "read_csv"
    print("✓ CSV reader works")
    
    # Test TSV reader
    reader = getreader("test.tsv")
    assert reader.__name__ == "read_tsv"
    print("✓ TSV reader works")
    
    # Test format override
    reader = getreader("test.txt", format_type="json")
    assert reader.__name__ == "read_json"
    print("✓ Format override works")
    
    # Test lazy mode
    reader = getreader("test.csv", lazy=True)
    assert reader.__name__ == "scan_csv"
    print("✓ Lazy mode works")
    
    # Test batch mode
    reader = getreader("test.csv", in_batch=True)
    assert reader.__name__ == "read_csv_batched"
    print("✓ Batch mode works")

def test_error_handling():
    """Test error handling."""
    print("\nTesting error handling...")
    
    # Test empty file path
    try:
        getreader("")
        assert False, "Should have raised TypeError"
    except TypeError as e:
        assert "file_path cannot be empty" in str(e)
        print("✓ Empty file path error handling works")
    
    # Test invalid format in focus mode
    try:
        getreader("test.xyz", focus=True)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unsupported format 'xyz' in focus mode" in str(e)
        print("✓ Focus mode error handling works")
    
    # Test empty format type
    try:
        getreader("test.csv", format_type="  ")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "format_type cannot be empty string" in str(e)
        print("✓ Empty format type error handling works")

def test_fallback_behavior():
    """Test fallback behavior for unknown formats."""
    print("\nTesting fallback behavior...")
    
    import warnings
    
    # Test normal fallback
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        reader = getreader("test.unknown")
        assert reader.__name__ == "read_csv"
        assert len(w) == 1
        assert "Unknown format 'unknown'" in str(w[0].message)
        print("✓ Normal fallback works")
    
    # Test lazy fallback
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        reader = getreader("test.unknown", lazy=True)
        assert reader.__name__ == "scan_csv"
        assert len(w) == 1
        assert "Unknown format 'unknown'" in str(w[0].message)
        print("✓ Lazy fallback works")

def main():
    """Run all tests."""
    print("Running tests for optimized getreader function...\n")
    
    try:
        test_basic_functionality()
        test_error_handling()
        test_fallback_behavior()
        print("\n🎉 All tests passed! The optimized function works correctly.")
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())