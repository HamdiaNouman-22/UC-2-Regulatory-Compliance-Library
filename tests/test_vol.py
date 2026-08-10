import sys
import logging
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

logging.basicConfig(level=logging.WARNING)
sys.path.insert(0, '.')

BASE_URL = 'https://cbben.thomsonreuters.com'

s = requests.Session()
s.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

url = 'https://cbben.thomsonreuters.com/rulebook/common-volume'
soup = BeautifulSoup(s.get(url, timeout=30).content, 'lxml')
target = url.rstrip('/')

print('Searching for target:', target)

found = False

for nav in soup.find_all('nav', id=re.compile(r'book-block-menu-')):
    for li in nav.find_all('li'):
        a = li.find('a', href=True)
        if not a:
            continue

        full = urljoin(BASE_URL, a['href']).rstrip('/')

        if full == target:
            print('FOUND li classes:', li.get('class'))

            child_ul = li.find('ul', recursive=False)
            print('child_ul:', child_ul is not None)

            if child_ul:
                lis = child_ul.find_all('li', recursive=False)
                print('Direct children:', len(lis))

                for cli in lis:
                    ca = cli.find('a', href=True)
                    text = ca.get_text(strip=True) if ca else 'NO-A'
                    print(f"  {cli.get('class')} | {text}")

            found = True
            break

    if found:
        break

if not found:
    print('NOT FOUND - checking all li hrefs:')

    for nav in soup.find_all('nav', id=re.compile(r'book-block-menu-'))[:2]:
        for li in nav.find_all('li')[:5]:
            a = li.find('a', href=True)
            if a:
                print(
                    ' ',
                    urljoin(BASE_URL, a['href']),
                    '|',
                    a.get_text(strip=True)[:40]
                )