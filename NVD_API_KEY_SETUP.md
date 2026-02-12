# How to Set Up NVD API Key for GitHub Actions

## Why You Need This

Without an NVD API key, the OWASP dependency-check plugin takes **20+ minutes** to download the CVE database from NVD. With an API key, this reduces to **under 5 minutes**.

## Steps to Get NVD API Key (FREE)

1. **Go to NVD Website**
   - Visit: https://nvd.nist.gov/developers/request-an-api-key

2. **Request API Key**
   - Click "Request an API Key"
   - Fill out the form with your email address
   - Accept the terms of use
   - Submit the form

3. **Check Your Email**
   - You'll receive an email with your API key within minutes
   - The API key looks like: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

## Steps to Add API Key to GitHub Repository

### Option 1: Repository Secret (Recommended)

1. **Navigate to Your Repository Settings**
   - Go to your GitHub repository: https://github.com/YOUR_USERNAME/LedgerForge
   - Click on **Settings** tab
   - In the left sidebar, click **Secrets and variables** → **Actions**

2. **Add New Repository Secret**
   - Click **New repository secret** button
   - Name: `NVD_API_KEY`
   - Value: Paste your API key (the one from the email)
   - Click **Add secret**

3. **Done!**
   - The GitHub workflow is already configured to use this secret
   - Next time you create a PR, the CVE scan will use the API key automatically

### Option 2: Organization Secret (If you have an organization)

1. Go to your GitHub organization settings
2. Navigate to **Secrets and variables** → **Actions**
3. Click **New organization secret**
4. Name: `NVD_API_KEY`
5. Value: Paste your API key
6. Repository access: Select repositories that can access this secret
7. Click **Add secret**

## How to Verify It's Working

1. Create a new Pull Request
2. The CVE scan workflow will run automatically
3. Check the workflow logs - you should see:
   ```
   [INFO] Using NVD API key for faster downloads
   [INFO] NVD CVE Database update completed in ~3-5 minutes
   ```
   Instead of 20+ minutes

## Rate Limits

- **Without API Key**: 5 requests per 30 seconds (very slow)
- **With API Key**: 50 requests per 30 seconds (10x faster)

This is why the API key dramatically speeds up the scan.

## Troubleshooting

### Pipeline still taking too long?

1. Check if the secret is named exactly: `NVD_API_KEY` (case-sensitive)
2. Ensure the workflow has permission to access secrets
3. Check workflow logs for errors related to API key authentication

### Secret not being picked up?

- Make sure the secret is added at the **repository** level, not just your personal account
- The workflow file is already configured correctly - no changes needed

## Security Notes

- ✅ GitHub Secrets are encrypted and never exposed in logs
- ✅ The API key is only used during workflow execution
- ✅ The API key is never committed to your repository
- ✅ You can rotate/regenerate the API key anytime from NVD website

## Cost

**FREE** - NVD API keys are completely free for public use.
