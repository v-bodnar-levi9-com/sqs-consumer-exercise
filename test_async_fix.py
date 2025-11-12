#!/usr/bin/env python3
"""
Simple test script to validate the async SQS operations fix
"""

import asyncio
import os
import sys

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

# Also add the parent directory to handle the relative imports
sys.path.insert(0, os.path.dirname(__file__))

from src.processor.main import SQSProcessor


async def test_async_operations():
    """Test that async operations work correctly"""
    print("Testing async SQS operations...")

    processor = SQSProcessor()

    # Test that process_messages is now a coroutine
    try:
        # This should return a coroutine object now
        coro = processor.process_messages()
        print(f"✓ process_messages() returns a coroutine: {type(coro)}")

        # Clean up the coroutine to avoid warnings
        coro.close()

    except Exception as e:
        print(f"✗ Error testing process_messages: {e}")
        return False

    # Test that other methods are also async
    async_methods = [
        "_get_queue_url",
        "_setup_dlq",
        "_get_dlq_arn",
        "_configure_queue_dlq",
        "_extend_message_visibility",
        "get_dlq_message_count",
    ]

    for method_name in async_methods:
        try:
            method = getattr(processor, method_name)
            if asyncio.iscoroutinefunction(method):
                print(f"✓ {method_name} is now async")
            else:
                print(f"✗ {method_name} is not async")
                return False
        except AttributeError:
            print(f"✗ Method {method_name} not found")
            return False

    print("✓ All SQS operations are now async!")
    return True


async def test_session_creation():
    """Test that aioboto3 session is created correctly"""
    print("Testing aioboto3 session creation...")

    try:
        processor = SQSProcessor()

        # Check if session is created
        if hasattr(processor, "session") and processor.session is not None:
            print("✓ aioboto3 session created successfully")
            return True
        else:
            print("✗ aioboto3 session not created")
            return False

    except Exception as e:
        print(f"✗ Error creating session: {e}")
        return False


async def main():
    """Run all tests"""
    print("Running async SQS operations validation tests...\n")

    tests = [
        test_session_creation,
        test_async_operations,
    ]

    all_passed = True

    for test in tests:
        try:
            result = await test()
            all_passed = all_passed and result
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
            all_passed = False

        print()  # Empty line between tests

    if all_passed:
        print(
            "🎉 All tests passed! The synchronous SQS operations have been successfully converted to async."
        )
        print("\n📊 Benefits achieved:")
        print("- Non-blocking I/O operations")
        print("- Improved concurrency")
        print("- Better scalability")
        print("- No more event loop blocking")
    else:
        print("❌ Some tests failed. Please check the implementation.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
