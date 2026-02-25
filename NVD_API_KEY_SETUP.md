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

### Getting 403/404 errors from NVD?

This is the most common issue. Here's how to fix it:

**1. Verify your API key is valid:**
```bash
# Test your API key with curl
curl -H "apiKey: YOUR_API_KEY_HERE" \
  "https://services.nvd.nist.gov/rest/json/cves/2.0?resultsPerPage=1"

# If valid, you'll see JSON response with CVE data
# If invalid, you'll see 403 Forbidden or 404 Not Found
```

**2. Check if your API key is expired:**
- NVD API keys can expire or be revoked
- Request a new key at: https://nvd.nist.gov/developers/request-an-api-key
- Replace the old key in GitHub Secrets

**3. Verify the secret is correctly set in GitHub:**
- Go to: Repository → Settings → Secrets and variables → Actions
- Ensure the secret name is exactly `NVD_API_KEY` (case-sensitive)
- Click "Update" and re-paste your API key (remove any extra spaces)

**4. For local testing:**
```bash
# Test with your API key
mvn clean verify -Dnvd.apiKey=YOUR_API_KEY_HERE

# If it works locally but fails in CI, the secret isn't configured correctly
```

**5. Wait and retry:**
- NVD occasionally has service outages
- The workflow now retries 3 times with backoff
- If all retries fail, wait 30 minutes and re-run the workflow

### Secret not being picked up?

- Make sure the secret is added at the **repository** level, not just your personal account
- The workflow file is already configured correctly - no changes needed

### Common mistakes:

1. **Trailing spaces in API key** - When pasting, ensure no spaces before/after the key
2. **Using an old/expired key** - Request a fresh key
3. **Fork PR** - Secrets aren't available to workflows from forked PRs for security
4. **Rate limiting** - Even with a key, making too many requests too fast causes 403

## Security Notes

- ✅ GitHub Secrets are encrypted and never exposed in logs
- ✅ The API key is only used during workflow execution
- ✅ The API key is never committed to your repository
- ✅ You can rotate/regenerate the API key anytime from NVD website

## Cost

**FREE** - NVD API keys are completely free for public use.
