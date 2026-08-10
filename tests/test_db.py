# save as tests/check_capital_market.py
import requests
from bs4 import BeautifulSoup

url = "https://cbben.thomsonreuters.com/cbb-capital-market-regulations"
resp = requests.get(url, headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})
print(f"Status: {resp.status_code}")
soup = BeautifulSoup(resp.text, "lxml")

# Check all navs
navs = soup.find_all("nav")
print(f"Navs found: {len(navs)}")
for nav in navs:
    print(f"  nav id={nav.get('id')} class={nav.get('class')}")

# Check all links with /rulebook/ or /cbb-capital
links = soup.find_all("a", href=True)
rulebook_links = [a for a in links if "capital" in a["href"].lower() or "rulebook" in a["href"].lower()]
print(f"\nCapital/rulebook links: {len(rulebook_links)}")
for a in rulebook_links[:20]:
    print(f"  {a['href']} — {a.get_text(strip=True)[:60]}")