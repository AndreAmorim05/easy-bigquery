# Security Policy

## Supported Versions

Use this section to tell people about which versions of your project are
currently being supported with security updates.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

We take the security of Easy BigQuery seriously. If you believe you have found a security vulnerability, please report it to us as described below.

## Preferred Languages

We prefer all communications to be in English or Portuguese.

## Policy

We are committed to working with security researchers to resolve any issues that are reported to us. We will:

* Acknowledge receipt of your vulnerability report within 48 hours
* Provide an initial assessment of the report within 1 week
* Keep you informed of our progress toward a fix and full announcement
* Credit you in the security advisory, unless you prefer otherwise

## Security Best Practices

When using Easy BigQuery, please follow these security best practices:

1. **Never commit credentials to version control**

   * Use environment variables or secure secret management
   * Store credentials in the `secrets/` directory (already in .gitignore)

2. **Use least privilege principle**
   * Grant only necessary permissions to your service account
   * Regularly review and rotate credentials

3. **Validate input data**
   * Always validate and sanitize data before uploading to BigQuery
   * Use parameterized queries when possible

4. **Monitor access**
   * Enable BigQuery audit logs
   * Monitor for unusual access patterns

5. **Keep dependencies updated**
   * Regularly update the library and its dependencies
   * Monitor for security advisories in dependencies

## Security Considerations

Easy BigQuery handles sensitive operations including:

* **Authentication**: Service account credentials management
* **Data Access**: Reading and writing data to BigQuery
* **Network Communication**: API calls to Google Cloud services

The library is designed with security in mind, but users should:

* Review the source code for their specific use cases
* Test in isolated environments before production use
* Follow Google Cloud security best practices
* Implement proper access controls and monitoring

## Disclosure Policy

When we receive a security bug report, we will:

1. Confirm the problem and determine the affected versions
2. Audit code to find any similar problems
3. Prepare fixes for all supported versions
4. Release new versions with the fixes
5. Publicly announce the vulnerability and the fix

## Credits

We would like to thank all security researchers who responsibly disclose vulnerabilities to us. Your contributions help make Easy BigQuery more secure for everyone.
