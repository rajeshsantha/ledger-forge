# Fixing NVD 403/404 API Errors - Complete Guide

## The Error You're Seeing

```
Error updating the NVD Data; the NVD returned a 403 or 404 error
Please ensure your API Key is valid
```

This means the OWASP Dependency-Check plugin cannot download CVE data from the National Vulnerability Database (NVD) because:
1. The API key is missing, invalid, or expired
2. The API key isn't being passed correctly to the plugin
3. NVD is rate-limiting or temporarily unavailable

---

## ✅ Solution Steps (In Order)

### Step 1: Verify Your NVD API Key is Valid

**Test your API key directly:**
```bash
# Replace YOUR_API_KEY with your actual key
curl -H "apiKey: YOUR_API_KEY_HERE" \
  "https://services.nvd.nist.gov/rest/json/cves/2.0?resultsPerPage=1"
```

**Expected responses:**
- ✅ **Valid key**: JSON response with CVE data
- ❌ **Invalid key**: `403 Forbidden` or `404 Not Found`
- ❌ **Expired key**: `403 Forbidden`

**If your key is invalid or expired:**
1. Go to: https://nvd.nist.gov/developers/request-an-api-key
2. Request a new API key
3. Check your email for the new key
4. Update the key in GitHub Secrets (next step)

---

### Step 2: Update/Verify GitHub Secret

1. Go to your repository: `https://github.com/YOUR_USERNAME/LedgerForge`
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Look for `NVD_API_KEY` in the list
4. Click **Update** (or **New repository secret** if missing)
5. **Name**: `NVD_API_KEY` (MUST be exactly this, case-sensitive)
6. **Value**: Paste your API key (remove any extra spaces/newlines)
7. Click **Update secret** / **Add secret**

**Common mistakes:**
- ❌ Name is wrong (e.g., `NVD_API`, `NVDAPIKEY`, `nvd_api_key`)
- ❌ Extra spaces before/after the key
- ❌ Using an old/expired key
- ❌ Secret is at organization level, not repository level

---

### Step 3: Test Locally First

Before running in CI, test on your local machine:

```bash
cd /path/to/LedgerForge

# Test with your API key
mvn clean verify -Dnvd.apiKey=YOUR_API_KEY_HERE

# Expected result:
# - Downloads NVD data successfully
# - Scans dependencies
# - Either succeeds or fails with CVE findings (not 403/404)
```

**If this works locally but fails in CI:**
- The GitHub Secret is not configured correctly
- Go back to Step 2 and double-check the secret name

**If this also fails locally:**
- Your API key is invalid - request a new one (Step 1)
- OR NVD is temporarily down - wait 30 minutes and retry

---

### Step 4: Understanding the Workflow Configuration

The workflow now includes these fixes:

```yaml
env:
  NVD_API_KEY: ${{ secrets.NVD_API_KEY }}

run: |
  mvn -B -DskipTests verify -Dnvd.apiKey="${NVD_API_KEY}"
```

And the `pom.xml` includes:

```xml
<nvdApiKey>${nvd.apiKey}</nvdApiKey>
<nvdApiDelay>6000</nvdApiDelay>
<nvdMaxRetryCount>10</nvdMaxRetryCount>
```

This configuration:
- Reads the secret from GitHub Actions
- Passes it to Maven as a system property
- Plugin picks it up and uses it for NVD API calls
- Waits 6 seconds between requests to avoid rate limiting
- Retries up to 10 times if NVD returns temporary errors

---

### Step 5: Re-run the Workflow

1. Push a commit to your PR branch (or close/reopen the PR)
2. Watch the Actions tab
3. Look for this log line:
   ```
   NVD_API_KEY is set (masked).
   ```
4. The scan should now complete in 3-5 minutes (instead of 20+)

**If you still see 403/404:**
- Check the Actions logs for the exact error
- The API key might be rate-limited - wait 1 hour and retry
- The NVD service might be down - check: https://nvd.nist.gov/

---

## 🔧 Advanced Troubleshooting

### Option 1: Disable NVD Updates (Temporary Workaround)

If NVD is completely unavailable, you can disable it temporarily:

Add to `pom.xml` plugin configuration:
```xml
<nvdDatafeedEnabled>false</nvdDatafeedEnabled>
<nvdApiDatafeedEnabled>false</nvdApiDatafeedEnabled>
```

**WARNING**: This will use cached/old CVE data and may miss recent vulnerabilities.

---

### Option 2: Use Hosted NVD Mirror (Advanced)

If your organization has an NVD mirror:

```xml
<nvdApiDatafeedUrl>https://your-nvd-mirror.example.com/api</nvdApiDatafeedUrl>
```

---

### Option 3: Increase Delays for Strict Rate Limits

If you're hitting rate limits even with a valid key:

```xml
<nvdApiDelay>10000</nvdApiDelay> <!-- Increase to 10 seconds -->
<nvdMaxRetryCount>15</nvdMaxRetryCount>
```

---

## 📊 How to Know It's Working

### Success indicators:

**In CI logs:**
```
NVD_API_KEY is set (masked).
Running dependency-check (attempt 1/3)...
[INFO] Checking for updates
[INFO] NVD CVE data last updated: 2026-02-12
[INFO] Check for updates complete (3252 ms)
dependency-check succeeded
```

**Total time:** 3-5 minutes (with API key) vs 20+ minutes (without)

**Build result:** ✅ PASS (if no high CVEs) or ❌ FAIL with CVE report (not 403/404 error)

---

## ❓ FAQ

### Q: Do I need to pay for an NVD API key?
**A:** No, it's completely FREE.

### Q: How do I know if my key is expired?
**A:** Test it with the curl command in Step 1. If you get 403, it's expired/invalid.

### Q: Can I use the same key for multiple projects?
**A:** Yes, one key works across all your projects.

### Q: Why doesn't my forked PR have access to secrets?
**A:** GitHub doesn't expose secrets to workflows from forked PRs for security reasons.

### Q: How long does an NVD API key last?
**A:** Typically 1 year, but check the email from NVD for expiration date.

### Q: Can I commit the API key to the repo?
**A:** ❌ NO! Always use GitHub Secrets. Never commit API keys to source control.

---

## 🎯 Quick Checklist

Before asking for help, verify:

- [ ] I requested an NVD API key from https://nvd.nist.gov/developers/request-an-api-key
- [ ] I received the key via email
- [ ] I tested the key with curl (Step 1) - it works
- [ ] The key works locally with `mvn verify -Dnvd.apiKey=...`
- [ ] I added the secret to GitHub: Settings → Secrets → Actions
- [ ] The secret name is EXACTLY `NVD_API_KEY` (case-sensitive)
- [ ] I clicked "Update secret" after pasting the value
- [ ] I'm testing from a branch in the same repo (not a fork)
- [ ] I pushed a commit to trigger the workflow
- [ ] I checked the Actions logs for "NVD_API_KEY is set"

If all boxes are checked and it still fails, paste the full error log and I'll help debug further.

---

## 📞 Still Need Help?

Provide this information:
1. Output of: `curl -H "apiKey: YOUR_KEY" "https://services.nvd.nist.gov/rest/json/cves/2.0?resultsPerPage=1"`
2. Does `mvn verify -Dnvd.apiKey=YOUR_KEY` work locally?
3. Screenshot of GitHub Secrets showing `NVD_API_KEY` exists
4. Full error log from GitHub Actions workflow

---

## ✨ Summary

The 403/404 error means the NVD API key isn't working. Follow these steps:

1. ✅ Get a valid API key from NVD
2. ✅ Test it with curl to confirm it works
3. ✅ Add it to GitHub Secrets as `NVD_API_KEY`
4. ✅ Push a commit to trigger the workflow
5. ✅ Scan completes in 3-5 minutes

**Your pom.xml and workflow are already configured correctly - you just need a valid API key in GitHub Secrets!**
