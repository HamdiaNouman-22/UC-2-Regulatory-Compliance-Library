# save as tests/check_capital_market6.py
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

BASE_URL = "https://cbben.thomsonreuters.com"

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Fetch main page
url = "https://cbben.thomsonreuters.com/cbb-capital-market-regulations"
resp = requests.get(url, headers=headers)
soup = BeautifulSoup(resp.text, "lxml")

marketlist = soup.find("ul", class_="marketlist")
print(f"Marketlist found: {marketlist is not None}")

if marketlist:
    for a in marketlist.find_all("a", href=True):
        href = a["href"]
        title = a.get_text(strip=True)
        print(f"\nSection: {title}")
        print(f"  href: {href}")
        
        if not (href.startswith("/node/") or href.startswith("/rulebook/")):
            print(f"  SKIP (external link)")
            continue
            
        full_url = urljoin(BASE_URL, href)
        
        # Check for entiresection
        section_resp = requests.get(full_url, headers=headers)
        section_soup = BeautifulSoup(section_resp.text, "lxml")
        
        for a2 in section_soup.find_all("a", href=True):
            if "entiresection" in a2["href"]:
                print(f"  entiresection: {urljoin(BASE_URL, a2['href'])}")
                break
        else:
            print(f"  NO entiresection found")