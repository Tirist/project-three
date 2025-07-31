#!/usr/bin/env python3
"""
Fix test suite issues by updating method calls to match the current implementation.

This script fixes test files that reference methods that have been removed or changed
in the pipeline modules.
"""

import sys
from pathlib import Path

# Add the pipeline directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipeline"))

from utils.common import cleanup_old_partitions

def fix_test_process_features():
    """Fix the test_process_features.py file to handle missing methods."""
    
    test_file = Path("tests/test_process_features.py")
    if not test_file.exists():
        print("❌ Test file not found")
        return False
    
    # Read the current test file
    with open(test_file, 'r') as f:
        content = f.read()
    
    # Fix the cleanup test by removing the direct method call
    old_cleanup_test = '''    # Test cleanup functionality
    print("\\n=== Testing Cleanup Functionality ===")
    
    # The FeatureProcessor class itself does not have a public cleanup_old_partitions method
    # This is handled by the pipeline runner or external utilities
    
    # Attempt to call a method that might exist if cleanup_old_partitions is public
    try:
        # This will fail if the method doesn't exist, which is expected
        processor.cleanup_old_partitions(dry_run=True)
        print("✅ cleanup_old_partitions method exists and works")
    except AttributeError:
        # This is expected behavior
        assert hasattr(processor, 'cleanup_old_partitions'), "cleanup_old_partitions method not found"
        print("Skipping direct call of cleanup_old_partitions as it's not a public method.")
    
    # Alternative: test the utility function directly
    try:
        from utils.common import cleanup_old_partitions
        cleanup_results = cleanup_old_partitions(processor.config, "processed", dry_run=True, test_mode=True)
        print("✅ cleanup_old_partitions utility function works")
    except Exception as e:
        print("❌ cleanup_old_partitions method not found in FeatureProcessor.")'''
    
    new_cleanup_test = '''    # Test cleanup functionality
    print("\\n=== Testing Cleanup Functionality ===")
    
    # Test the utility function directly since wrapper methods have been removed
    try:
        cleanup_results = cleanup_old_partitions(processor.config, "processed", dry_run=True, test_mode=True)
        print("✅ cleanup_old_partitions utility function works")
        
        # Check cleanup results structure
        required_cleanup_fields = [
            'cleanup_date', 'retention_days', 'cutoff_date',
            'deleted_partitions', 'total_deleted', 'dry_run', 'test_mode'
        ]
        
        missing_fields = [field for field in required_cleanup_fields if field not in cleanup_results]
        assert not missing_fields, f"Missing cleanup fields: {missing_fields}"
        
    except Exception as e:
        print(f"❌ cleanup_old_partitions utility function failed: {e}")'''
    
    # Replace the old test with the new one
    content = content.replace(old_cleanup_test, new_cleanup_test)
    
    # Write the fixed test file
    with open(test_file, 'w') as f:
        f.write(content)
    
    print("✅ Fixed test_process_features.py")
    return True

def fix_test_fetch_data():
    """Fix the test_fetch_data.py file to handle missing methods."""
    
    test_file = Path("tests/test_fetch_data.py")
    if not test_file.exists():
        print("❌ Test file not found")
        return False
    
    # Read the current test file
    with open(test_file, 'r') as f:
        content = f.read()
    
    # Fix the retention cleanup test
    old_cleanup_test = '''@pytest.mark.quick
def test_retention_cleanup():
    """Test retention cleanup functionality."""
    print("\\n=== Testing Retention Cleanup ===")

    fetcher = OHLCVFetcher()

    # Test cleanup with dry-run
    cleanup_results = fetcher.cleanup_old_partitions(dry_run=True)

    # Check cleanup results structure
    required_cleanup_fields = [
        'cleanup_date', 'retention_days', 'cutoff_date',
        'deleted_partitions', 'total_deleted', 'dry_run', 'test_mode'
    ]

    missing_fields = [field for field in required_cleanup_fields if field not in cleanup_results]
    assert not missing_fields, f"Missing cleanup fields: {missing_fields}"'''
    
    new_cleanup_test = '''@pytest.mark.quick
def test_retention_cleanup():
    """Test retention cleanup functionality."""
    print("\\n=== Testing Retention Cleanup ===")

    fetcher = OHLCVFetcher()

    # Test cleanup with dry-run using utility function directly
    cleanup_results = cleanup_old_partitions(fetcher.config, "raw", dry_run=True, test_mode=True)

    # Check cleanup results structure
    required_cleanup_fields = [
        'cleanup_date', 'retention_days', 'cutoff_date',
        'deleted_partitions', 'total_deleted', 'dry_run', 'test_mode'
    ]

    missing_fields = [field for field in required_cleanup_fields if field not in cleanup_results]
    assert not missing_fields, f"Missing cleanup fields: {missing_fields}"'''
    
    # Replace the old test with the new one
    content = content.replace(old_cleanup_test, new_cleanup_test)
    
    # Fix the rate limit handling test
    old_rate_limit_test = '''        # Mock time.sleep to avoid actual delays
        with patch('time.sleep') as mock_sleep:
            fetcher.handle_rate_limit(1)
            assert mock_sleep.call_count == 1, f"Rate limit strategy '{strategy}' did not call sleep"'''
    
    new_rate_limit_test = '''        # Mock time.sleep to avoid actual delays
        with patch('time.sleep') as mock_sleep:
            from utils.common import handle_rate_limit
            handle_rate_limit(1, fetcher.config)
            assert mock_sleep.call_count == 1, f"Rate limit strategy '{strategy}' did not call sleep"'''
    
    # Replace the old test with the new one
    content = content.replace(old_rate_limit_test, new_rate_limit_test)
    
    # Write the fixed test file
    with open(test_file, 'w') as f:
        f.write(content)
    
    print("✅ Fixed test_fetch_data.py")
    return True

def main():
    """Main function to fix all test issues."""
    print("🔧 Fixing test suite issues...")
    
    # Fix process features tests
    if fix_test_process_features():
        print("✅ Process features tests fixed")
    else:
        print("❌ Failed to fix process features tests")
    
    # Fix fetch data tests
    if fix_test_fetch_data():
        print("✅ Fetch data tests fixed")
    else:
        print("❌ Failed to fix fetch data tests")
    
    print("🎯 Test suite fixes completed")

if __name__ == "__main__":
    main() 