# Contributing to Easy BigQuery

Thank you for considering contributing to **Easy BigQuery**! This document provides guidelines and important information for contributors.

## Table of Contents

- [How to Contribute](#how-to-contribute)
- [Development Environment Setup](#development-environment-setup)
- [Code Standards](#code-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Requesting Features](#requesting-features)
- [Code of Conduct](#code-of-conduct)

## How to Contribute

There are several ways to contribute to the project:

- **Report bugs and/or suggest new features** through [Issues](https://github.com/AndreAmorim05/easy-bigquery/issues)
- **Submit Pull Requests** with fixes or improvements
- **Improve documentation**
- **Add tests**

## Development Environment Setup

### Prerequisites

- Python 3.13+
- Poetry (dependency manager) or equivalent (uv, pdm, etc.)
- Git

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/AndreAmorim05/easy-bigquery.git
   cd easy-bigquery
   ```

2. **Install dependencies:**

   ```bash
   poetry install --with dev
   ```

3. **Activate the virtual environment:**

   ```bash
   poetry shell
   ```

4. **Configure environment variables:**

   Create a `.env` file in the `secrets/` folder with the following variables:

   ```env
   BQ_PROJECT_ID=your-gcp-project
   BQ_DATASET=your-dataset
   BQ_TABLE_NAME=your-table
   BQ_JSON_CREDENTIALS='{"type": "service_account", ...}'
   ```

### Project Structure

```bash
easy-bigquery/
├── easy_bigquery/         # Main source code
│   ├── connector/         # Connection management
│   ├── context/           # Context manager
│   ├── core/              # Configuration and utilities
│   ├── logger/            # Logging system
│   └── workers/           # Workers for fetch and push
├── tests/                 # Unit tests
├── docs/                  # Documentation
└── secrets/               # Configuration files
```

## Code Standards

### Formatting

The project uses the following tools to maintain code quality:

- **Blue**: Code formatting (Black-compatible)
- **isort**: Import organization
- **Line length**: 79 characters

### Running Linting

```bash
# Check formatting
poetry run task lint

# Format code automatically
poetry run blue .
poetry run isort .
```

### Naming Conventions

- **Classes**: PascalCase (e.g., `BQConnector`, `FetchWorker`)
- **Functions and methods**: snake_case (e.g., `connect()`, `fetch_data()`)
- **Variables**: snake_case (e.g., `project_id`, `credentials_info`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `BQ_PROJECT_ID`)

### Code Documentation

- Use Google Style docstrings
- Document all public methods
- Include usage examples when appropriate

**Example:**

```python
def fetch(self, query: str, **kwargs: Any) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a DataFrame.

    Args:
        query: The SQL string to execute.
        **kwargs: Additional arguments for the to_dataframe() method.

    Returns:
        A pandas DataFrame containing the query results.

    Raises:
        RuntimeError: If the BigQuery client is not available.
    """
```

## Testing

### Running Tests

```bash
# Run all tests
poetry run task test

# Run tests with coverage
poetry run pytest --cov=easy_bigquery --cov-report=html

# Run specific tests
poetry run pytest tests/connector/test_connector.py

# Run integration tests
poetry run task test-integration
```

### Testing Standards

- Use **pytest** as the testing framework
- Use **pytest-mock** for mocking
- Organize tests in files that mirror the source code structure
- Use fixtures for common setup
- Test both success and error cases

### Test Example

```python
def test_connector_connect(mock_connector_tuple):
    """Test if the connect() method instantiates and assigns clients correctly."""
    connector, mocks = mock_connector_tuple
    
    # Execute the method
    connector.connect()
    
    # Verify calls
    mocks['client_class'].assert_called_once_with(
        credentials=mocks['credentials'], 
        project='test-project'
    )
    
    # Verify state
    assert connector.client is mocks['client_instance']
```

### Test Coverage

- Maintain test coverage above 80%
- Run `poetry run task test` before submitting a PR
- Tests are automatically run in CI/CD

## Documentation

### API Documentation

- Use detailed docstrings for all public classes and methods
- Include practical usage examples
- Document parameters, return types, and exceptions

### Project Documentation

- Keep README.md updated
- Document important changes in CHANGELOG.md
- Use MkDocs for technical documentation

### Running Documentation Locally

```bash
# Install documentation dependencies
poetry install --with doc

# Run documentation server
poetry run task docs
```

## Pull Request Process

### Before Submitting a PR

1. **Create a branch from main:**

   ```bash
   git checkout -b feature/new-feature
   ```

2. **Make your changes following the standards:**
   - Format the code: `poetry run task lint`
   - Run tests: `poetry run task test`
   - Update documentation if necessary

3. **Commit your changes:**

   ```bash
   git add .
   git commit -m "feat: add new feature X"
   ```

### Commit Conventions

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `style:` Code formatting
- `refactor:` Code refactoring
- `test:` Adding or fixing tests
- `chore:` Maintenance tasks

### Submitting the PR

1. **Push to your branch:**

   ```bash
   git push origin feature/new-feature
   ```

2. **Create a Pull Request on GitHub:**
   - Use the provided PR template
   - Clearly describe the changes
   - Reference related issues
   - Include tests if applicable

3. **Wait for review:**
   - All tests must pass
   - Code must follow standards
   - At least one maintainer must approve

## Reporting Bugs

### Before Reporting

1. Check if the bug has already been reported
2. Test with the latest version
3. Reproduce the problem in a clean environment

### Bug Report Template

```markdown
**Bug Description**
A clear and concise description of what happened.

**To Reproduce**
1. Go to '...'
2. Click on '...'
3. Scroll down to '...'
4. See error

**Expected Behavior**
A clear description of what should happen.

**Screenshots**
If applicable, add screenshots.

**Environment:**
 - OS: [e.g., Ubuntu 20.04]
 - Python: [e.g., 3.13.0]
 - Easy BigQuery: [e.g., 0.1.1-alpha]

**Additional Context**
Any other information about the problem.
```

## Requesting Features

### Feature Request Template

```markdown
**Problem the feature would solve**
A clear description of the problem you're facing.

**Proposed Solution Description**
A clear description of the feature you would like.

**Alternatives Considered**
A description of any alternative solutions you considered.

**Additional Context**
Any additional context, screenshots, etc.
```

## Contribution Checklist

Before submitting your contribution, verify that:

- [ ] Code follows formatting standards
- [ ] Tests pass locally
- [ ] Documentation has been updated
- [ ] Commit message follows conventions
- [ ] Feature has been adequately tested
- [ ] No merge conflicts

## Code of Conduct

### Our Standards

- Be respectful and inclusive
- Use welcoming language
- Accept constructive criticism
- Focus on what is best for the community
- Show empathy toward other community members

### Our Responsibilities

Project maintainers are responsible for:

- Clarifying acceptable behavior standards
- Taking appropriate and fair corrective action
- Removing, editing, or rejecting comments, commits, code, and other contributions

## Support

If you have questions about contributing:

- Open an [Issue](https://github.com/AndreAmorim05/easy-bigquery/issues)
- Consult the [documentation](https://easy-bigquery.readthedocs.io/)
- Contact the maintainers

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE.txt).

---

Thank you for contributing to Easy BigQuery! 🚀
