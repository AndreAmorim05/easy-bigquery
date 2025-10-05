# Contributing to Easy BigQuery

Thank you for considering contributing! We welcome improvements, bug fixes, tests, and documentation. Please read below to understand how to get involved.



## Getting Started

1. **Fork** this repository and clone your fork locally.
2. Create a new branch for your work:
   ```bash
   git checkout -b feature/my-feature
   ```
4. Make your changes in the new branch.

## Pull Request Guidelines

When submitting a PR, please follow these conventions:

- **Title**:
    ```
    type(scope): short description
    ```
    Examples:
    - `fix(query): handle empty string columns`
    - `feat(schema): support clustering configuration`
    - `docs: update usage examples`

- **PR Body**:
    ```
    ### Description (REQUIRED)

    <!-- brief description of what this PR does -->

    ### Related Issues (Optional)

    <!-- referenced issues (e.g., `Closes #12`) -->

    ### Additional notes (Optional)

    <!-- any additional context -->
    ```

## How You Can Contribute

We welcome contributions such as:

- Reporting reproducible bugs via [GitHub Issues](https://github.com/yourorg/easy-bigquery/issues)
- Submitting new features or enhancements
- Improving test coverage
- Improving or clarifying documentation
- Reviewing existing pull requests

## PR Checklist

Before you submit, please confirm:

 - [ ] You have read the contribution guidelines
 - [ ] You have tested the changes locally
 - [ ] You have updated the documentation where needed
 - [ ] You have described what this PR does
 - [ ] You have ensured my changes do not break existing functionality
 - [ ] You have added any new dependencies to `requirements.txt` or `pyproject.toml`
 - [ ] You have checked for any merge conflicts
 - [ ] You have ensured my code is compatible with Python 3.13+
 - [ ] You have ensured my code is compatible with the latest version of BigQuery
 - [ ] You have ensured my code is compatible with the latest version of Google Cloud SDK
 - [ ] You have ensured my code is compatible with the latest version of Google Cloud Python libraries
 - [ ] You have ensured my code is compatible with the latest version of Google Cloud BigQuery API

## Questions?

If you have questions or need guidance, please open an issue or discuss with us via GitHub. We are happy to help!