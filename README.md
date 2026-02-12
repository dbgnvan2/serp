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
   pip install google-search-results pandas openpyxl wordcloud matplotlib python-dotenv textblob
   ```

3. Set your API Key:
   ```bash
   export SERPAPI_KEY="your_api_key_here"
   ```

## Running the Tool

```bash
python serp_audit.py
```

## Testing

To run the regression tests:

```bash
python -m unittest test_serp_audit.py
```
