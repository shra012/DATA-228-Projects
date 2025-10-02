# MapReduce with MongoDB

This project uses [uv](https://github.com/astral-sh/uv) to manage Python versions, virtual environments, and dependencies. The repository does **not** include a checked-in virtual environment, so you will need to create one locally before installing packages.

## Setup

1. **Create a Virtual Environment**
   ```bash
   uv venv .venv
   ```

2. **Install Dependencies**
   ```bash
   uv pip sync --preview-features pylock -p .venv pylock.toml
   ```

3. **Activate the Virtual Environment**
   ```bash
   source .venv/bin/activate
   ```

4. **Run the MapReduce Pipeline**
   With the virtual environment active and a MongoDB server listening on `localhost:27017`, execute:
   ```bash
   python map_reduce.py
   ```
   Alternatively, use uv without activating the venv:
   ```bash
   uv run --python .venv/bin/python map_reduce.py
   ```

## What the Pipeline Does

The script loads the sample `orders.json` dataset into MongoDB, performs a MapReduce job that aggregates item counts, totals, and averages by category, writes the results to the `category_sales` collection, and prints a formatted summary of those aggregates.
