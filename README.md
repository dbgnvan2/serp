# serp

## Overview

A Market Intelligence Tool designed to support the "Bridge Strategy" for a non-profit counselling agency. It maps "Problem-Aware" queries to "Solution-Aware" content using SERP data.

## Installation

**Prerequisites:**

- Python 3.8+
- A valid SerpApi Key

1. Create a virtual environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Set your API Key:
   ```bash
   export SERPAPI_KEY="your_api_key_here"
   ```

## Running the Tool

To execute the full pipeline (Audit -> Validation -> Verification):

```bash
python run_pipeline.py
```

## Utilities

**Visualize Volatility:** Plot rank history for a keyword.

```bash
python visualize_volatility.py --list
python visualize_volatility.py --keyword "Free counselling North Vancouver"
```

**Export History:** Dump SQLite database to CSVs.

```bash
python export_history.py
```

## Testing

To run the regression tests:

```bash
python -m unittest test_serp_audit.py
```
