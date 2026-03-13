import os
import sys

# Set required env vars BEFORE any imports that trigger pydantic-settings
os.environ.setdefault("AGENT_API_URL", "https://test.easyalert.io")
os.environ.setdefault("AGENT_API_KEY", "ea_agent_test_key_12345")

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
