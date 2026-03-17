"""
Legacy entrypoint.

This project was refactored into a modular pipeline:
  - extract.py
  - transform.py
  - load.py
  - main.py

To run the pipeline, prefer:
  python scripts/main.py
"""

from main import main


if __name__ == "__main__":
    main()

