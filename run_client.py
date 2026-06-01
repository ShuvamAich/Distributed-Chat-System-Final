"""Quick-launch script for a single client instance."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client.chat_client import main
main()
