# Some museum WAFs reject outdated Chrome versions (Chrome/122 started drawing
# 403s in Sept 2026), so keep this close to the current stable Chrome release.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/154.0.0.0 Safari/537.36"
)
