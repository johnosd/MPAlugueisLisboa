# Repo Guidelines

This project contains Python code, notebooks, and data for predicting rental prices in Lisbon.

## Working with the repository

- All Python source files live inside the `src` directory.
- Keep code formatted according to [PEP 8](https://peps.python.org/pep-0008/). When possible use `black` for formatting.
- Commit messages should be brief and written in English.
- Run the test suite using `pytest` after making code changes. If there are no tests, `pytest` will exit without running anything.
- Install dependencies from `src/requirements.txt` if needed using:
  ```bash
  pip install -r src/requirements.txt
  ```

