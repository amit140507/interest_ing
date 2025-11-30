from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import mysql.connector
import re
import time

# ----------------- DB connection -----------------
def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="interest_ing"
    )

# ----------------- Parse tenor to days -----------------
import re

def parse_tenor(tenor_text):
    """
    Convert a tenor string (like '7 - 14 Days', '391 Days - Less than 23 Months', 
    '3 years and above but less than 4 years') into min_days, max_days (integers).
    """
    tenor_text = tenor_text.lower().replace("and above", "").replace("upto and inclusive of", "")
    tenor_text = tenor_text.replace("less than", "").replace("than", "").strip()
    
    
    # Helper functions
    def convert_years(text):
        m = re.search(r"(\d+)\s*year", text)
        return int(m.group(1))*365 if m else None
    
    def convert_months(text):
        m = re.search(r"(\d+)\s*month", text)
        return int(m.group(1))*30 if m else None
    
    def convert_days(text):
        m = re.search(r"(\d+)\s*day", text)
        return int(m.group(1)) if m else None
    
    # Split into min/max parts
    if "to" in tenor_text:
        left, right = [x.strip() for x in tenor_text.split("to")]
    elif "-" in tenor_text:
        left, right = [x.strip() for x in tenor_text.split("-", 1)]
    else:
        left = right = tenor_text
    
    # Convert each part to days
    def text_to_days(text):
        val = convert_days(text) or convert_months(text) or convert_years(text)
        return val
    
    left_val = text_to_days(left)
    right_val = text_to_days(right)
    
    # If right_val is None, assume same as left_val
    if right_val is None:
        right_val = left_val
    
    return left_val, right_val


# ----------------- Fetch Kotak FD table using Selenium -----------------
def fetch_kotak_fd_rates():
    # Setup headless Chrome
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    service = Service()  # make sure chromedriver is in PATH
    driver = webdriver.Chrome(service=service, options=options)
    
    url = "https://www.kotak.bank.in/en/rates/interest-rates.html"
    driver.get(url)
    
    # Wait for JS to render the first rate-details section
    time.sleep(5)  # simple wait; can be improved with WebDriverWait
    
    # Get page source
    html = driver.page_source
    driver.quit()
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    
    # Locate first rate-details table div
    rate_div = soup.find("div", class_="ratedetails")
    if not rate_div:
        print("Rate table not found")
        return []
    
    rates = []
    table = rate_div.find("table")
    if not table:
        print("Table not found inside ratedetails div")
        return []
    
    for tr in table.find_all("tr")[1:]:  # skip header
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue
        tenor = cols[0].get_text(strip=True)
        rate_text = cols[2].get_text(strip=True).replace("%","").strip()
        try:
            rate = float(rate_text)
        except ValueError:
            continue
        min_days, max_days = parse_tenor(tenor)
        rates.append({
            "tenor": tenor,
            "min_days": min_days,
            "max_days": max_days,
            "interest_rate": rate
        })
    
    return rates

# ----------------- Insert rates into MySQL -----------------
def insert_kotak_rates(rates):
    db = get_db()
    cursor = db.cursor()
    
    # Get or create bank
    cursor.execute("SELECT id FROM banks WHERE name='Kotak Mahindra Bank'")
    res = cursor.fetchone()
    if res:
        bank_id = res[0]
    else:
        cursor.execute("INSERT INTO banks(name) VALUES ('Kotak Mahindra Bank')")
        db.commit()
        bank_id = cursor.lastrowid
    
    # Optional: remove old rates
    cursor.execute("DELETE FROM interest_rates WHERE bank_id=%s", (bank_id,))
    db.commit()
    
    for r in rates:
        cursor.execute("""
            INSERT INTO interest_rates (bank_id, min_days, max_days, interest_rate)
            VALUES (%s, %s, %s, %s)
        """, (bank_id, r["min_days"], r["max_days"], r["interest_rate"]))
    
    db.commit()
    cursor.close()
    db.close()
    print(f"Inserted {len(rates)} rates for Kotak Mahindra Bank")

# ----------------- Main -----------------
if __name__ == "__main__":
    fd_rates = fetch_kotak_fd_rates()
    print("Fetched rates:")
    for r in fd_rates:
        print(r)
    # if fd_rates:
        # insert_kotak_rates(fd_rates)
