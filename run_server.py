"""Quick-launch script for a single server instance."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.chat_server import main
main()
