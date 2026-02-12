#!/usr/bin/env python3
"""CLI wrapper for metrics processor."""

import sys

from documentai_api.tasks.metrics_processor.main import main

if __name__ == "__main__":
    sys.exit(main())
