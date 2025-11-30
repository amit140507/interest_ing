import requests
from bs4 import BeautifulSoup
import mysql.connector
import re

# -------------------------------
# 1. MySQL CONNECTION
# -------------------------------
def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="interest_ing"
    )

# -------------------------------
# 2. PARSE TENOR → min_days, max_days
# -------------------------------
def parse_tenor(tenor_text):
    tenor_text = tenor_text.lower().strip()
    tenor_text = tenor_text.replace("less than", "").replace("less", "").replace("than", "").strip()

    # Helper: convert "1 year", "2 years" → days
    def convert_years(text):
        m = re.search(r"(\d+)\s*year", text)
        return int(m.group(1)) * 365 if m else None

    # Helper: convert "211 days" → days
    def convert_days(text):
        m = re.search(r"(\d+)\s*day", text)
        return int(m.group(1)) if m else None

    # Split on "to"
    if "to" not in tenor_text:
        # Single item like "7 days"
        d = convert_days(tenor_text) or convert_years(tenor_text)
        return d, d

    left, right = [x.strip() for x in tenor_text.split("to")]

    # Extract numeric values
    left_days = convert_days(left)
    right_days = convert_days(right)

    left_years = convert_years(left)
    right_years = convert_years(right)

    # LEFT SIDE
    if left_days is not None:
        min_days = left_days
    elif left_years is not None:
        min_days = left_years
    else:
        min_days = None

    # RIGHT SIDE
    if right_days is not None:
        max_days = right_days
    elif right_years is not None:
        max_days = right_years
    else:
        max_days = None

    return min_days, max_days


def fetch_sbi_fd_rates():
    url = "https://sbi.bank.in/web/interest-rates/deposit-rates/retail-domestic-term-deposits"
    resp = requests.get(url)
    resp.raise_for_status()
    html = resp.text

    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table")
    results = []

    for table in tables:
        headers = table.find_all("th")
        if not headers:
            continue

        # Identify the main FD table
        if "tenors" in headers[0].text.lower():
            rows = table.find_all("tr")[1:]  # skip header
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue

                tenor = cols[0].get_text(strip=True)
                rate = cols[2].get_text(strip=True).replace("%", "").strip()

                min_days, max_days = parse_tenor(tenor)
                if min_days is None:
                    continue  # skip unparsed

                results.append({
                    "min_days": min_days,
                    "max_days": max_days,
                    "rate": float(rate)
                })

    return results

# -------------------------------
# 4. INSERT INTO MYSQL
# -------------------------------
def insert_into_database(data):
    db = get_db()
    cursor = db.cursor()

    # Get SBI bank_id (or insert if not exist)
    cursor.execute("SELECT id FROM banks WHERE name='SBI'")
    res = cursor.fetchone()

    if res:
        bank_id = res[0]
    else:
        cursor.execute("INSERT INTO banks (name) VALUES ('SBI')")
        db.commit()
        bank_id = cursor.lastrowid

    # Clear old SBI data (optional)
    cursor.execute("DELETE FROM interest_rates WHERE bank_id=%s", (bank_id,))
    db.commit()

    # Insert new data
    for row in data:
        cursor.execute("""
            INSERT INTO interest_rates (bank_id, min_days, max_days, interest_rate)
            VALUES (%s, %s, %s, %s)
        """, (bank_id, row["min_days"], row["max_days"], row["rate"]))

    db.commit()
    cursor.close()
    db.close()
    print("SBI Rates updated successfully!")


# -------------------------------
# 5. MAIN RUN
# -------------------------------
if __name__ == "__main__":
    print("Fetching SBI FD Rates...")
    sbi_data = fetch_sbi_fd_rates()
    print("Found:", len(sbi_data), "records")

    print("Inserting into database...")
    insert_into_database(sbi_data)

    print("Done.")
