# sbp_crawler.py
import re
import time
import urllib.parse
import requests
from bs4 import BeautifulSoup
from crawler.crawler import BaseCrawler
from models import RegulatoryDocument


class SBPCrawler(BaseCrawler):
    ROOT_URL = "https://www.sbp.org.pk"
    CIRCULARS_URL = ROOT_URL + "/circulars/cir.asp"
    NOTIFICATIONS_URL = ROOT_URL + "/circulars/notifications.asp"
    REGULATORY_RETURNS_URL = ROOT_URL + "/Regulatory_Returns/index.asp"

    def __init__(self, timeout: int = 120, retries: int = 3, backoff: float = 1.5):
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

    # SAFE REQUEST WITH RETRY
    def _get_soup(self, url: str):
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                return BeautifulSoup(resp.text, "html.parser")

            except Exception as e:
                print(f"[SBP] _get_soup failed ({attempt}/{self.retries}) → {url}")
                print(f"       {e}")

                if attempt == self.retries:
                    raise

                time.sleep(self.backoff * attempt)

    def _abs_url(self, base: str, href: str):
        return urllib.parse.urljoin(base, href)

    def _valid_url(self, href: str):
        if not href:
            return False
        if href.lower().startswith("javascript"):
            return False
        return True

    def _get_circular_departments(self):
        soup = self._get_soup(self.CIRCULARS_URL)

        dept_table = soup.find("table", attrs={"bordercolor": re.compile("E8E8E8", re.I)})
        if not dept_table:
            print("[SBP] ERROR: Circular departments table not found")
            return []

        departments = []
        for a in dept_table.find_all("a", href=True):
            href = a["href"].strip()

            if not href.lower().endswith("/index.htm"):
                continue

            # Exclude navigation garbage
            if "whatnew" in href.lower() or "index.asp" in href.lower():
                continue

            full_url = self._abs_url(self.CIRCULARS_URL, href)
            departments.append({
                "name": a.get_text(strip=True),
                "url": full_url
            })

        print(f"[SBP] Circular departments: {len(departments)}")
        return departments

    def _get_years_from_dept(self, dept):
        soup = self._get_soup(dept["url"])
        years = []

        for a in soup.find_all("a", href=True):
            if not re.fullmatch(r"\d{4}", a.get_text(strip=True)):
                continue

            years.append({
                "department": dept["name"],
                "year": a.get_text(strip=True),
                "url": self._abs_url(dept["url"], a["href"])
            })

        return years

    def _parse_circular_year(self, year_info):
        soup = self._get_soup(year_info["url"])
        tables = soup.find_all("table")
        if not tables:
            return []

        main_table = max(tables, key=lambda t: len(t.find_all("tr")))
        docs = []

        for tr in main_table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) != 4:
                continue

            circ_no = tds[0].get_text(strip=True)
            title_text = tds[2].get_text(strip=True)


            if not circ_no:
                continue

            if "what's new" in circ_no.lower() or "what's new" in title_text.lower():
                continue

            if "homeabout" in circ_no.lower() or "publications" in circ_no.lower():
                continue

            if not any(tag in circ_no.lower() for tag in
                       ["circular", "letter", "acr", "acfid", "bprd", "ih&", "fd", "smfd", "mfd"]):
                continue

            link = tds[2].find("a", href=True)
            if not link:
                continue

            href = link["href"]
            if not self._valid_url(href):
                continue

            if "whatnew" in href.lower():
                continue

            html_url = self._abs_url(year_info["url"], href)

            # Urdu version (optional)
            urdu_link = tds[3].find("a", href=True)
            urdu_url = (
                self._abs_url(year_info["url"], urdu_link["href"])
                if urdu_link and self._valid_url(urdu_link["href"])
                else None
            )

            docs.append({
                "regulator": "SBP",
                "department": year_info["department"],
                "year": year_info["year"],

                "circular_no": circ_no,
                "date": tds[1].get_text(strip=True),
                "title": link.get_text(strip=True),

                "english_url": html_url,
                "urdu_url": urdu_url,
                "year_page_url": year_info["url"]
            })

        return docs

    def get_circulars(self):
        all_docs = []
        for dept in self._get_circular_departments():
            for y in self._get_years_from_dept(dept):
                try:
                    docs = self._parse_circular_year(y)
                    all_docs.extend(docs)
                except Exception as e:
                    print(f"[SBP] ERROR parsing circular year {y['year']}: {e}")

        print(f"[SBP] Total circulars: {len(all_docs)}")
        return all_docs


    def _get_notification_departments(self):
        soup = self._get_soup(self.NOTIFICATIONS_URL)

        dept_table = soup.find("table", attrs={"bordercolor": re.compile("E8E8E8", re.I)})
        if not dept_table:
            print("[SBP] ERROR: Notification department table missing")
            return []

        departments = []
        for a in dept_table.find_all("a", href=True):
            if not a["href"].lower().endswith("index.htm"):
                continue

            full = self._abs_url(self.NOTIFICATIONS_URL, a["href"])
            departments.append({"name": a.get_text(strip=True), "url": full})

        print(f"[SBP] Notification departments: {len(departments)}")
        return departments

    def _parse_notification_year(self, year_info):
        soup = self._get_soup(year_info["url"])
        tables = soup.find_all("table")
        if not tables:
            return []

        main_table = max(tables, key=lambda t: len(t.find_all("tr")))
        docs = []

        for tr in main_table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) != 3:
                continue

            notif_no = tds[0].get_text(strip=True)
            if not notif_no:
                continue

            date = tds[1].get_text(strip=True)

            link = tds[2].find("a", href=True)
            if not link:
                continue

            href = link["href"]
            if not self._valid_url(href):
                continue

            html_url = self._abs_url(year_info["url"], href)

            docs.append({
                "regulator": "SBP",
                "department": year_info["department"],
                "year": year_info["year"],

                "notification_no": notif_no,
                "date": date,
                "title": link.get_text(strip=True),

                # MAIN URL ALWAYS HTML
                "english_url": html_url,
                "urdu_url": None,

                "year_page_url": year_info["url"]
            })

        return docs

    def get_notifications(self):
        all_docs = []
        for dept in self._get_notification_departments():
            soup = self._get_soup(dept["url"])

            # Extract years
            years = []
            for a in soup.find_all("a", href=True):
                if re.fullmatch(r"\d{4}", a.get_text(strip=True)):
                    years.append({
                        "department": dept["name"],
                        "year": a.get_text(strip=True),
                        "url": self._abs_url(dept["url"], a["href"])
                    })

            for y in years:
                try:
                    docs = self._parse_notification_year(y)
                    all_docs.extend(docs)
                except Exception as e:
                    print(f"[SBP] ERROR parsing notification year {y['year']}: {e}")

        print(f"[SBP] Total notifications: {len(all_docs)}")
        return all_docs


    def _get_regulatory_return_departments(self):
        soup = self._get_soup(self.REGULATORY_RETURNS_URL)

        dept_table = soup.find("table", attrs={"bordercolor": re.compile("E8E8E8", re.I)})
        if not dept_table:
            print("[SBP] ERROR: Regulatory return dept table missing")
            return []

        departments = []
        for a in dept_table.find_all("a", href=True):
            href = a["href"].strip().lower()

            if not href.endswith(".htm"):
                continue
            if "index" in href or "whatnew" in href:
                continue

            full = self._abs_url(self.REGULATORY_RETURNS_URL, href)
            departments.append({"name": a.get_text(strip=True), "url": full})

        print(f"[SBP] Regulatory Return Departments: {len(departments)}")
        return departments

    def _parse_regulatory_return_department(self, dept):
        soup = self._get_soup(dept["url"])
        tables = soup.find_all("table")
        docs = []

        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 3:
                continue

            header = " ".join(td.get_text(strip=True).lower() for td in rows[1].find_all("td"))

            # We want the table with Statement + Circular
            if "statement" not in header and "return" not in header:
                continue
            if "circular" not in header:
                continue

            for tr in rows[2:]:
                tds = tr.find_all("td")
                if len(tds) < 5:
                    continue

                stmt_link = tds[1].find("a", href=True)
                statement_url = None
                statement_name = None

                if stmt_link and self._valid_url(stmt_link["href"]):
                    statement_url = self._abs_url(dept["url"], stmt_link["href"])
                    statement_name = stmt_link.get_text(strip=True)
                else:
                    statement_name = tds[1].get_text(strip=True)

                circ_link = tds[2].find("a", href=True)
                circular_html = None
                circular_ref = None

                if circ_link and self._valid_url(circ_link["href"]):
                    circular_html = self._abs_url(dept["url"], circ_link["href"])
                    circular_ref = circ_link.get_text(strip=True)
                else:
                    circular_ref = tds[2].get_text(strip=True)

                docs.append({
                    "regulator": "SBP",
                    "department": dept["name"],

                    "statement_name": statement_name,
                    "statement_url": statement_url,

                    "title": circular_ref,
                    "reference_no": circular_ref,

                    "english_url": circular_html,
                    "urdu_url": None,

                    "circular_html_url": circular_html,
                    "xls_url": statement_url,

                    "frequency": tds[3].get_text(strip=True),
                    "due_date": tds[4].get_text(strip=True),
                    "submission_mode": tds[5].get_text(strip=True),

                    "source_page_url": dept["url"],
                    "year": None,
                    "published_date": None,

                    "category": "Regulatory Return",
                })

        return docs

    def get_regulatory_returns(self):
        all_docs = []

        for dept in self._get_regulatory_return_departments():
            try:
                docs = self._parse_regulatory_return_department(dept)
                all_docs.extend(docs)
            except Exception as e:
                print(f"[SBP] ERROR parsing Regulatory Return dept {dept['name']}: {e}")

        print(f"[SBP] Total regulatory returns: {len(all_docs)}")
        return all_docs

    def _map_to_reg_doc(self, item: dict, category: str):
        return RegulatoryDocument(
            regulator="SBP",
            source_system="SBP-" + category.upper(),
            category=category,

            title=item["title"],
            document_url=item.get("english_url"),
            urdu_url=item.get("urdu_url"),

            published_date=item.get("date") or item.get("published_date"),
            reference_no=(
                item.get("circular_no")
                or item.get("notification_no")
                or item.get("reference_no")
            ),

            department=item.get("department"),
            year=item.get("year"),
            source_page_url=item.get("year_page_url") or item.get("source_page_url"),

            file_type=None,
            extra_meta=item
        )

    def get_documents(self):
        docs = []

        print("\n===== FETCHING SBP CIRCULARS =====")
        for item in self.get_circulars():
            docs.append(self._map_to_reg_doc(item, "Circular"))

        print("\n===== FETCHING SBP NOTIFICATIONS =====")
        for item in self.get_notifications():
            docs.append(self._map_to_reg_doc(item, "Notification"))

        print("\n===== FETCHING SBP REGULATORY RETURNS =====")
        for item in self.get_regulatory_returns():
            docs.append(self._map_to_reg_doc(item, "Regulatory Return"))

        return docs
