---
name: Bug report
about: Create a report to help us improve
title: '[BUG] '
labels: ['bug']
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:

1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

- **Code Example**

```python
from easy_bigquery import BQManager

# Your code here
with BQManager() as bq:
    # This causes the error
    df = bq.fetch("SELECT * FROM my_table")
```

- **Error Message**

```bash
Paste the full error message here
```

- **Environment:**
OS: [e.g. Ubuntu 20.04, macOS 12.0, Windows 11]
Python Version: [e.g. 3.13.0]
Easy BigQuery Version: [e.g. 0.1.1-alpha]
Google Cloud BigQuery Version: [e.g. 3.31.0]

- **Additional context**
Add any other context about the problem here.

- **Checklist**
- [ ] I have searched existing issues to avoid duplicates
- [ ] I have provided a minimal code example that reproduces the issue
- [ ] I have included the full error message
- [ ] I have specified my environment details
