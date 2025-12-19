from flask import Flask, request, render_template, send_file, redirect, url_for, session, flash, jsonify
import pandas as pd
from io import BytesIO
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
import psycopg2
import time
import requests
from bs4 import BeautifulSoup
from flask import request, render_template_string
from collections import defaultdict
from sshtunnel import SSHTunnelForwarder
from typing import List, Tuple
from calendar import monthrange
import json
import os


def rgb_to_hex(rgb):
    r = int(rgb.get('red', 1) * 255)
    g = int(rgb.get('green', 1) * 255)
    b = int(rgb.get('blue', 1) * 255)
    return '#{:02X}{:02X}{:02X}'.format(r, g, b)
    
def load_department_order() -> List[str]:
    if not os.path.exists(DEPARTMENT_ORDER_PATH):
        return []
    try:
        with open(DEPARTMENT_ORDER_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return [str(item) for item in data]
    except Exception:
        pass
    return []


def persist_department_order(order: List[str]) -> None:
    with open(DEPARTMENT_ORDER_PATH, "w", encoding="utf-8") as f:
        json.dump(order, f)

def get_matching_google_sheet_rows(engine_code):
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)

        SPREADSHEET_ID = '1iH-70OrINA2jcd6YKszW-N8XpuJDTC9A3oArNWHbEeY'
        RANGE = 'Sheet1'

        service = build('sheets', 'v4', credentials=creds)

        values_result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
        values = values_result.get('values', [])

        format_result = service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID,
            ranges=[RANGE],
            fields='sheets.data.rowData.values.effectiveFormat.backgroundColor'
        ).execute()

        row_data = format_result['sheets'][0]['data'][0]['rowData']

        headers = values[0]
        rows = []

        for i, row in enumerate(values[1:], start=1):
            row_dict = {}
            for j, cell in enumerate(row):
                if j in (17, 18):  # Skip columns R and S
                    continue
                cell_text = cell
                bg_color = row_data[i]['values'][j].get('effectiveFormat', {}).get('backgroundColor', {})
                hex_color = rgb_to_hex(bg_color)
                key = headers[j]
                row_dict[key] = {'value': cell_text, 'bg': hex_color}
            if any(engine_code.lower() in str(c).lower() for c in row):
                rows.append(row_dict)

        return rows

    except Exception as e:
        print("Error accessing Google Sheets:", e)
        return []

file_path = 'WebFleet.csv'
df = pd.read_csv(file_path)

app = Flask(__name__)
app.secret_key = 'your_super_secret_key_here'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEPARTMENT_ORDER_PATH = os.path.join(BASE_DIR, "department_order.json")

USERS = {
    'admin': 'Silverlake1!',
    'paul': 'Silverlake1!',
    'nacho': 'Silverlake1!'
}

last_search_result = None
search_details = None

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in USERS and USERS[username] == password:
            session['logged_in'] = True
            session['login_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            return redirect(url_for('index'))
        else:
            error = 'Invalid Credentials. Please try again.'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.before_request
def require_login():
    allowed_routes = ['login', 'static', 'autocomplete_model']
    if request.endpoint not in allowed_routes and not session.get('logged_in'):
        return redirect(url_for('login'))
    if session.get('logged_in'):
        login_time = session.get('login_time')
        if login_time:
            login_time = datetime.strptime(login_time, '%Y-%m-%d %H:%M:%S')
            if datetime.utcnow() - login_time > timedelta(hours=24):
                session.clear()
                return redirect(url_for('login'))

@app.route('/autocomplete_model', methods=['GET'])
def autocomplete_model():
    query = request.args.get('query', '')
    if query:
        filtered_models = df['Model'].dropna().unique()
        matches = [model for model in filtered_models if query.lower() in model.lower()]
        return {'models': matches}
    return {'models': []}

@app.route('/', methods=['GET', 'POST'])
def index():
    global last_search_result, search_details
    parts = None
    google_sheet_matches = []
    if request.method == 'POST':
        model = request.form['model']
        year = int(request.form['year'])
        engine_code = request.form.get('engine_code', '').strip()
        min_price = request.form.get('min_price')
        min_opportunity = request.form.get('min_opportunity')
        action = request.form.get('action')

        # Initial filtering
        filtered = df[
            (df['Model'].str.lower() == model.lower()) &
            (df['IC Start Year'] <= year) &
            (df['IC End Year'] >= year)
        ]

        if engine_code:
            def custom_filter(row):
                description = str(row['IC Description'])
                if 'engine code' in description.lower():
                    return engine_code.lower() in description.lower()
                return True
            filtered = filtered[filtered.apply(custom_filter, axis=1)]

        # 🚨 NEW: exclusion list logic
        if action == 'search_excluding':
            exclusion_keywords = [
                "ENGINE", "TRANS/GEARBOX", "TURBOCHARGER", "SUPERCHARGER", "THROTTLE_BODY",
                "ALTERNATOR", "STARTER", "A/C_COMPRESSOR", "Cylinder_head",
                "FUEL_INJECTOR", "Injector_rail", "COIL/COIL_PACK",
                "Injector_pump", "OIL_PAN/SUMP", "EGR_VALVE/COOLER"
            ]
            pattern = '|'.join(rf'\b{kw}\b' for kw in exclusion_keywords)
            filtered = filtered[~filtered['Part'].str.contains(pattern, case=False, na=False, regex=True)]

        # Proceed with opportunity calculations if there's something left
        if not filtered.empty:
            filtered['Potential_Profit'] = (filtered['Backorders'] + filtered['Not Found 180 days']) * filtered['B Price']
            filtered['Sales_Speed'] = filtered['Parts Sold All'] / (filtered['Parts in Stock'] + 1)
            filtered['Opportunity_Score'] = filtered['Potential_Profit'] * filtered['Sales_Speed']

            if min_price:
                filtered = filtered[filtered['B Price'] >= float(min_price)]
            if min_opportunity:
                filtered = filtered[filtered['Opportunity_Score'] >= float(min_opportunity)]

            parts = filtered[['Part', 'IC Start Year', 'IC End Year', 'IC Description', 'B Price', 'Parts in Stock', 'Backorders',
                              'Parts Sold All', 'Not Found 180 days', 'Potential_Profit', 'Sales_Speed', 'Opportunity_Score']]
            parts = parts.sort_values(by=['Backorders', 'Opportunity_Score'], ascending=False).head(50)
            last_search_result = parts
            search_details = {'model': model, 'year': year, 'engine_code': engine_code}
            parts = parts.to_dict('records')

        if engine_code:
            google_sheet_matches = get_matching_google_sheet_rows(engine_code)

    return render_template('index.html', parts=parts, search_details=search_details, google_sheet_matches=google_sheet_matches)

@app.route('/download')
def download():
    global last_search_result
    if last_search_result is not None:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            last_search_result.to_excel(writer, index=False, sheet_name='Parts')
        output.seek(0)
        return send_file(output, download_name="parts_opportunity.xlsx", as_attachment=True)
    return "No data to download", 400

@app.route('/ebay_small_parts')
def ebay_small_parts():
    import time
    model = request.args.get('model', '').strip()
    year = request.args.get('year', '').strip()
    if not model or not year:
        return "Model and year are required.", 400

    query = f"{model} {year}"
    search_url = (
        "https://www.ebay.co.uk/sch/131090/i.html?_nkw=" + query.replace(" ", "+") +
        "&LH_ItemCondition=4&rt=nc&_sop=12&_udhi=50&LH_Complete=1&LH_Sold=1"
    )
    print("\U0001F50D eBay search URL:", search_url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    response = None
    for attempt in range(3):
        try:
            response = requests.get(search_url, headers=headers, timeout=10)
            response.raise_for_status()
            break
        except Exception as e:
            print(f"eBay fetch attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    else:
        return render_template_string("<p><strong>Failed to fetch data from eBay after 3 attempts.</strong></p>")

    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.select('.s-item')
    print(f"Found {len(items)} items in eBay search Small.")

    part_list = []

    for item in items:
        title_tag = item.select_one('.s-item__title')
        price_tag = item.select_one('.s-item__price')
        link_tag = item.select_one('.s-item__link')

        if not title_tag or not price_tag or not link_tag:
            continue

        title = title_tag.get_text(strip=True)
        price_text = price_tag.get_text(strip=True).replace("£", "").split()[0]
        link = link_tag.get("href")

        try:
            price = float(price_text)
        except ValueError:
            continue

        if price <= 50:
            part_list.append({
                "title": title,
                "price": price,
                "link": link
            })

    if not part_list:
        return "<p>No results found under £50.</p>"

    part_list.sort(key=lambda x: x["price"], reverse=True)

    html = "<table class='table table-striped'><thead><tr><th>Title</th><th>Price</th><th>Link</th></tr></thead><tbody>"
    for part in part_list:
        html += f"<tr><td>{part['title']}</td><td>£{part['price']:.2f}</td><td><a href='{part['link']}' target='_blank'>View</a></td></tr>"
    html += "</tbody></table>"

    return render_template_string(html)

@app.route('/ebay_medium_parts')
def ebay_medium_parts():
    import time
    model = request.args.get('model', '').strip()
    year = request.args.get('year', '').strip()
    if not model or not year:
        return "Model and year are required.", 400

    query = f"{model} {year}"
    search_url = (
        "https://www.ebay.co.uk/sch/131090/i.html?_nkw=" + query.replace(" ", "+") +
        "&LH_ItemCondition=4&rt=nc&_sop=12&_udlo=50&_udhi=500&LH_Complete=1&LH_Sold=1"
        
    )
    print("\U0001F50D eBay search URL:", search_url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    response = None
    for attempt in range(3):
        try:
            response = requests.get(search_url, headers=headers, timeout=10)
            response.raise_for_status()
            break
        except Exception as e:
            print(f"eBay fetch attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    else:
        return render_template_string("<p><strong>Failed to fetch data from eBay after 3 attempts.</strong></p>")

    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.select('.s-item')
    print(f"Found {len(items)} items in eBay search Medium.")

    part_list = []

    for item in items:
        title_tag = item.select_one('.s-item__title')
        price_tag = item.select_one('.s-item__price')
        link_tag = item.select_one('.s-item__link')

        if not title_tag or not price_tag or not link_tag:
            continue

        title = title_tag.get_text(strip=True)
        price_text = price_tag.get_text(strip=True).replace("£", "").split()[0]
        link = link_tag.get("href")

        try:
            price = float(price_text)
        except ValueError:
            continue

        if price > 50 and price <= 500:
            part_list.append({
                "title": title,
                "price": price,
                "link": link
            })

    if not part_list:
        return "<p>No results found between £50 and £500.</p>"

    part_list.sort(key=lambda x: x["price"], reverse=True)

    html = "<table class='table table-striped'><thead><tr><th>Title</th><th>Price</th><th>Link</th></tr></thead><tbody>"
    for part in part_list:
        html += f"<tr><td>{part['title']}</td><td>£{part['price']:.2f}</td><td><a href='{part['link']}' target='_blank'>View</a></td></tr>"
    html += "</tbody></table>"

    return render_template_string(html)

@app.route('/ebay_large_parts')
def ebay_large_parts():
    import time
    model = request.args.get('model', '').strip()
    year = request.args.get('year', '').strip()
    if not model or not year:
        return "Model and year are required.", 400

    query = f"{model} {year}"
    search_url = (
        "https://www.ebay.co.uk/sch/131090/i.html?_nkw=" + query.replace(" ", "+") +
        "&LH_ItemCondition=4&rt=nc&_sop=12&_udlo=500&_udhi=5000&LH_Complete=1&LH_Sold=1"
    )
    print("\U0001F50D eBay search URL:", search_url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    response = None
    for attempt in range(3):
        try:
            response = requests.get(search_url, headers=headers, timeout=20)
            response.raise_for_status()
            break
        except Exception as e:
            print(f"eBay fetch attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    else:
        return render_template_string("<p><strong>Failed to fetch data from eBay after 3 attempts.</strong></p>")

    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.select('.s-item')
    print(f"Found {len(items)} items in eBay search Large.")

    part_list = []

    for item in items:
        title_tag = item.select_one('.s-item__title')
        price_tag = item.select_one('.s-item__price')
        link_tag = item.select_one('.s-item__link')

        if not title_tag or not price_tag or not link_tag:
            continue

        title = title_tag.get_text(strip=True)
        price_text = price_tag.get_text(strip=True).replace("£", "").split()[0]
        link = link_tag.get("href")

        try:
            price = float(price_text)
        except ValueError:
            continue

        if price >= 500:
            part_list.append({
                "title": title,
                "price": price,
                "link": link
            })

    if not part_list:
        return "<p>No results found over £500.</p>"

    part_list.sort(key=lambda x: x["price"], reverse=True)

    html = "<table class='table table-striped'><thead><tr><th>Title</th><th>Price</th><th>Link</th></tr></thead><tbody>"
    for part in part_list:
        html += f"<tr><td>{part['title']}</td><td>£{part['price']:.2f}</td><td><a href='{part['link']}' target='_blank'>View</a></td></tr>"
    html += "</tbody></table>"

    return render_template_string(html)

# PostgreSQL connection helper
# configure SSH and DB
SSH_HOST = "192.168.10.23"
SSH_PORT = 22
SSH_USER = "nacho"
SSH_KEY = None  # if using password auth, set to None
SSH_PASSWORD = "Ggbx*DPK8=4X!"  # or leave None if using key

DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "silverlake"
DB_USER = "postgres"
DB_PASS = ""

# keep tunnel global so it persists
tunnel = None

def init_ssh_tunnel():
    global tunnel
    if tunnel is None or not tunnel.is_active:
        tunnel = SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=("127.0.0.1", 5432)
        )
        tunnel.start()
        print(f"SSH tunnel established at 127.0.0.1:{tunnel.local_bind_port}")

def get_db_connection():
    init_ssh_tunnel()
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

@app.route("/crush_vehicles", methods=["GET", "POST"])
def crush_vehicles():
    vehicle = None
    
    error_message = None
      
    if request.method == "POST":
        reg = request.form.get("registration").strip()
        stock = request.form.get("stock_number").strip()

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT veh.stocknumber_id, veh.regnumber, st.vstockno, loc.bin
            FROM vehicle veh
            JOIN stocknumber st on st.stocknumber_id=veh.stocknumber_id
            LEFT JOIN location loc on loc.location_id=veh.location_id
            WHERE veh.regnumber = %s OR st.vstockno = %s
        """, (reg, stock))
        vehicle = cur.fetchone()

        cur.close()
        conn.close()
        
        
        user = session.get("username", "unknown")
        if vehicle:
            log_action("SEARCH", user, reg, stock, vehicle[2], vehicle[3], "FOUND")
        else:
            log_action("SEARCH", user, reg, stock, None, None, "NOT FOUND")
            error_message = f"Vehicle with the Registration '{reg}' OR Stock number '{stock}' haven’t been found on Pinnacle."

    return render_template("crush_vehicles.html", vehicle=vehicle, error_message=error_message)

@app.route("/crush/<int:vehicle_id>", methods=["POST"])
def crush(vehicle_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE vehicle SET location_id = %s WHERE stocknumber_id = %s", ("11045", vehicle_id))
    conn.commit()
    cur.close()
    conn.close()
    
    user = session.get("username", "unknown")
    log_action("CRUSH", user, None, None, None, None, "CRUSHED SUCCESSFULLY")
    flash("✅ Vehicle has been CRUSHED successfully!", "success")
    return redirect(url_for("crush_vehicles"))
    
def log_action(action, username, reg=None, stock=None, vstockno=None, location=None, status=None):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO public.hpd3281 (action, username, regnumber, stocknumber, vstockno, location, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (action, username, reg, stock, vstockno, location, status))
    conn.commit()
    cur.close()
    conn.close()


def fetch_department_sales(start_date: date, end_date: date) -> List[Tuple[str, float, float]]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT departmentname,
               SUM(total) AS sum_total,
               SUM(total + totaltax1) AS sum_total_vat
        FROM invoice
        WHERE datecreated >= %s AND datecreated < %s
        GROUP BY departmentname
        ORDER BY departmentname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_user_sales(start_date: date, end_date: date) -> List[Tuple[str, float, float]]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT us.shortname,
               SUM(total) AS sum_total,
               SUM(total + totaltax1) AS sum_total_vat
        FROM invoice
        JOIN pinuser us ON us.user_id = invoice.whocreated_id
        WHERE datecreated >= %s AND datecreated < %s
        GROUP BY us.shortname
        ORDER BY us.shortname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_department_parts_sold(start_date: date, end_date: date) -> List[Tuple[str, float, float]]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(inv.departmentname, 'Unknown') AS departmentname,
               COUNT(sold.invnumber) AS parts_sold
        FROM sold
        LEFT JOIN invoice inv ON inv.invoice_id = sold.invoice_id
        WHERE sold.issold AND solddate >= %s AND solddate < %s
        GROUP BY departmentname
        ORDER BY departmentname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # Reuse the same tuple shape as sales totals so the rest of the code can stay generic.
    return [(row[0], float(row[1]), float(row[1])) for row in rows]


def fetch_user_parts_sold(start_date: date, end_date: date) -> List[Tuple[str, float, float]]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(us.shortname, 'Unknown') AS shortname,
               COUNT(sold.invnumber) AS parts_sold
        FROM sold
        LEFT JOIN invoice inv ON inv.invoice_id = sold.invoice_id
        LEFT JOIN pinuser us ON us.user_id = inv.whocreated_id
        WHERE sold.issold AND solddate >= %s AND solddate < %s
        GROUP BY us.shortname
        ORDER BY us.shortname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [(row[0], float(row[1]), float(row[1])) for row in rows]


def shift_one_month_back(value: date) -> date:
    """Return the same calendar day in the previous month, clamped to month length."""

    year = value.year
    month = value.month - 1
    if month == 0:
        month = 12
        year -= 1

    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def fetch_department_monthly_totals(department: str, year: int) -> List[Tuple[int, float]]:
    start_year = date(year, 1, 1)
    start_next_year = date(year + 1, 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(MONTH FROM datecreated)::int AS month,
               SUM(total) AS sum_total
        FROM invoice
        WHERE datecreated >= %s
          AND datecreated < %s
          AND departmentname = %s
        GROUP BY month
        ORDER BY month
        """,
        (start_year, start_next_year, department),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_department_parts_monthly_totals(department: str, year: int) -> List[Tuple[int, float]]:
    start_year = date(year, 1, 1)
    start_next_year = date(year + 1, 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(MONTH FROM solddate)::int AS month,
               COUNT(sold.invnumber) AS parts_sold
        FROM sold
        LEFT JOIN invoice inv ON inv.invoice_id = sold.invoice_id
        WHERE solddate >= %s
          AND solddate < %s
          AND COALESCE(inv.departmentname, 'Unknown') = %s
          AND sold.issold
        GROUP BY month
        ORDER BY month
        """,
        (start_year, start_next_year, department),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_user_monthly_totals(user: str, year: int) -> List[Tuple[int, float]]:
    start_year = date(year, 1, 1)
    start_next_year = date(year + 1, 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(MONTH FROM datecreated)::int AS month,
               SUM(total) AS sum_total
        FROM invoice inv
        JOIN pinuser us ON us.user_id = inv.whocreated_id
        WHERE datecreated >= %s
          AND datecreated < %s
          AND us.shortname = %s
        GROUP BY month
        ORDER BY month
        """,
        (start_year, start_next_year, user),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_department_daily_totals(department: str, year: int, month: int) -> List[Tuple[int, float]]:
    start_month = date(year, month, 1)
    if month == 12:
        start_next_month = date(year + 1, 1, 1)
    else:
        start_next_month = date(year, month + 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(DAY FROM datecreated)::int AS day,
               SUM(total) AS sum_total
        FROM invoice
        WHERE datecreated >= %s
          AND datecreated < %s
          AND departmentname = %s
        GROUP BY day
        ORDER BY day
        """,
        (start_month, start_next_month, department),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_user_daily_totals(user: str, year: int, month: int) -> List[Tuple[int, float]]:
    start_month = date(year, month, 1)
    if month == 12:
        start_next_month = date(year + 1, 1, 1)
    else:
        start_next_month = date(year, month + 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(DAY FROM datecreated)::int AS day,
               SUM(total) AS sum_total
        FROM invoice inv
        JOIN pinuser us ON us.user_id = inv.whocreated_id
        WHERE datecreated >= %s
          AND datecreated < %s
          AND us.shortname = %s
        GROUP BY day
        ORDER BY day
        """,
        (start_month, start_next_month, user),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_department_parts_daily_totals(department: str, year: int, month: int) -> List[Tuple[int, float]]:
    start_month = date(year, month, 1)
    if month == 12:
        start_next_month = date(year + 1, 1, 1)
    else:
        start_next_month = date(year, month + 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(DAY FROM solddate)::int AS day,
               COUNT(sold.invnumber) AS parts_sold
        FROM sold
        LEFT JOIN invoice inv ON inv.invoice_id = sold.invoice_id
        WHERE solddate >= %s
          AND solddate < %s
          AND COALESCE(inv.departmentname, 'Unknown') = %s
          AND sold.issold
        GROUP BY day
        ORDER BY day
        """,
        (start_month, start_next_month, department),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
    
def parse_date_filter(filter_type: str, start_date_str: str = None, end_date_str: str = None) -> Tuple[date, date]:
    today = date.today()

    if filter_type == "today":
        return today, today + timedelta(days=1)
    if filter_type == "yesterday":
        return today - timedelta(days=1), today
    if filter_type == "this_month":
        return today.replace(day=1), (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    if filter_type == "last_month":
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - timedelta(days=1)
        return last_month_end.replace(day=1), first_this_month
    if filter_type == "custom" and start_date_str and end_date_str:
        return date.fromisoformat(start_date_str), date.fromisoformat(end_date_str)

    return today, today + timedelta(days=1)
    
    
def describe_date_range(filter_type: str, start_date: date, end_date: date) -> str:
    if not start_date or not end_date:
        return "All Time"

    labels = {
        "today": "Today",
        "yesterday": "Yesterday",
        "this_month": "This Month",
        "last_month": "Last Month",
        "custom": "Custom",
    }

    inclusive_end = end_date - timedelta(days=1)

    def format_date(value: date) -> str:
        return value.strftime("%d/%m/%Y")

    if start_date == inclusive_end:
        range_text = format_date(start_date)
    else:
        range_text = f"{format_date(start_date)} - {format_date(inclusive_end)}"

    label = labels.get(filter_type, "Custom")
    return f"{label} ({range_text})"

@app.route("/logs", methods=["GET", "POST"])
def logs():
    filter_type = request.args.get("filter", "today")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)

    today = date.today()

    if filter_type == "today":
        start_date = today
        end_date = today + timedelta(days=1)
    elif filter_type == "yesterday":
        start_date = today - timedelta(days=1)
        end_date = today
    elif filter_type == "this_month":
        start_date = today.replace(day=1)
        end_date = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    elif filter_type == "last_month":
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - timedelta(days=1)
        start_date = last_month_end.replace(day=1)
        end_date = first_this_month
    elif filter_type == "custom":
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

    conn = get_db_connection()
    cur = conn.cursor()
    if start_date and end_date:
        cur.execute("""
            SELECT to_char(timestamp, 'DD.MM.YYYY HH24:MI:SS') AS timestamp, username, action, regnumber, stocknumber, vstockno, location, status
            FROM public.hpd3281
            WHERE timestamp >= %s AND timestamp < %s
            ORDER BY timestamp DESC
        """, (start_date, end_date))
    else:
        cur.execute("""
            SELECT to_char(timestamp, 'DD.MM.YYYY HH24:MI:SS') AS timestamp, username, action, regnumber, stocknumber, vstockno, location, status
            FROM public.hpd3281
            ORDER BY timestamp DESC
            LIMIT 100
        """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("logs.html", logs=rows, filter_type=filter_type)


def normalize_stats_mode(mode: str) -> str:
    return "parts" if str(mode).lower() == "parts" else "sales"


def normalize_stats_dimension(dimension: str) -> str:
    return "user" if str(dimension).lower() == "user" else "department"


def build_stats_context(
    filter_type: str,
    start_date_str: str,
    end_date_str: str,
    exclude_args: List[str],
    mode: str,
    dimension: str,
):
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)

    resolved_mode = normalize_stats_mode(mode)
    resolved_dimension = normalize_stats_dimension(dimension)
    mode_label = "Parts Sold" if resolved_mode == "parts" else "Sales"
    value_format = "count" if resolved_mode == "parts" else "currency"
    value_label = "Parts Sold" if resolved_mode == "parts" else "Sales Total"
    value_vat_label = (
        "Parts Sold (Prev Period)" if resolved_mode == "parts" else "Total with VAT"
    )

    entity_label = "Department" if resolved_dimension == "department" else "User"
    entity_label_plural = "Departments" if resolved_dimension == "department" else "Users"

    if resolved_dimension == "department":
        fetch_rows = fetch_department_parts_sold if resolved_mode == "parts" else fetch_department_sales
        fetch_prev_rows = fetch_department_parts_sold
    else:
        fetch_rows = fetch_user_parts_sold if resolved_mode == "parts" else fetch_user_sales
        fetch_prev_rows = fetch_user_parts_sold

    rows = fetch_rows(start_date, end_date)
    prev_rows = []
    prev_row_map = {}
    if resolved_mode == "parts":
        prev_start = shift_one_month_back(start_date)
        prev_end = shift_one_month_back(end_date)
        prev_rows = fetch_prev_rows(prev_start, prev_end)
        prev_row_map = {row[0]: float(row[1]) for row in prev_rows}
    saved_order = load_department_order()
    order_index = {name: idx for idx, name in enumerate(saved_order)}

    exclusion_key = (
        "stats_excluded_departments"
        if resolved_dimension == "department"
        else "stats_excluded_users"
    )
    default_exclusions = session.get(exclusion_key, [])
    excluded_departments = exclude_args or default_exclusions

    filtered_rows = []
    for row in rows:
        if row[0] in excluded_departments:
            continue
        if resolved_mode == "parts":
            prev_value = prev_row_map.get(row[0], 0.0)
            filtered_rows.append((row[0], float(row[1]), float(prev_value)))
        else:
            filtered_rows.append(row)
    filtered_rows = sorted(
        filtered_rows,
        key=lambda row: (order_index.get(row[0], float("inf")), row[0]),
    )

    sum_total = sum(float(row[1]) for row in filtered_rows)
    sum_total_vat = sum(float(row[2]) for row in filtered_rows)

    chart_labels = [row[0] for row in filtered_rows]
    chart_values = [float(row[1]) for row in filtered_rows]

    all_departments = sorted({row[0] for row in rows})

    return {
        "filter_type": filter_type,
        "start_date": start_date,
        "end_date": end_date,
        "date_range_label": date_range_label,
        "rows": filtered_rows,
        "sum_total": sum_total,
        "sum_total_vat": sum_total_vat,
        "chart_labels": chart_labels,
        "chart_values": chart_values,
        "all_departments": all_departments,
        "excluded_departments": excluded_departments,
        "stats_mode": resolved_mode,
        "stats_dimension": resolved_dimension,
        "mode_label": mode_label,
        "value_format": value_format,
        "value_label": value_label,
        "value_vat_label": value_vat_label,
        "entity_label": entity_label,
        "entity_label_plural": entity_label_plural,
    }

    
@app.route("/stats", methods=["GET"])
def stats():
    filter_type = request.args.get("filter", "this_month")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    mode = request.args.get("mode", "sales")
    dimension = request.args.get("dimension", "department")

    context = build_stats_context(
        filter_type, start_date_str, end_date_str, excluded_args, mode, dimension
    )
    live_enabled = str(request.args.get("live", "")).lower() in {"1", "true", "yes", "on"}

    return render_template(
        "stats.html",
        **context,
        live_enabled=live_enabled,
    )


@app.route("/stats/data", methods=["GET"])
def stats_data():
    filter_type = request.args.get("filter", "this_month")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    mode = request.args.get("mode", "sales")
    dimension = request.args.get("dimension", "department")

    context = build_stats_context(
        filter_type, start_date_str, end_date_str, excluded_args, mode, dimension
    )

    return jsonify(
        {
            "date_range_label": context["date_range_label"],
            "rows": [
                {
                    "department": row[0],
                    "total": float(row[1]),
                    "total_vat": float(row[2]),
                }
                for row in context["rows"]
            ],
            "sum_total": context["sum_total"],
            "sum_total_vat": context["sum_total_vat"],
            "chart_labels": context["chart_labels"],
            "chart_values": context["chart_values"],
        }
    )


@app.route("/stats/order", methods=["POST"])
def save_department_order():
    data = request.get_json(silent=True) or {}
    order = data.get("order", [])

    if not isinstance(order, list):
        return jsonify({"error": "Invalid order payload"}), 400

    normalized_order = [str(item) for item in order]
    persist_department_order(normalized_order)

    return jsonify({"status": "saved", "order": normalized_order})
    
    
@app.route("/stats/department/<path:department>/monthly", methods=["GET"])
def stats_department_monthly(department):
    current_year = date.today().year
    mode = normalize_stats_mode(request.args.get("mode", "sales"))
    dimension = normalize_stats_dimension(request.args.get("dimension", "department"))
    if dimension == "user":
        fetch_rows = (
            fetch_user_parts_monthly_totals
            if mode == "parts"
            else fetch_user_monthly_totals
        )
    else:
        fetch_rows = (
            fetch_department_parts_monthly_totals
            if mode == "parts"
            else fetch_department_monthly_totals
        )

    rows = fetch_rows(department, current_year)

    labels = []
    values = []
    months = []
    for month, total in rows:
        labels.append(datetime(1900, month, 1).strftime("%b"))
        values.append(float(total))
        months.append(month)

    return jsonify({"labels": labels, "values": values, "months": months, "year": current_year})


@app.route("/stats/department/<path:department>/daily", methods=["GET"])
def stats_department_daily(department):
    try:
        month = int(request.args.get("month", "1"))
    except ValueError:
        month = 1

    current_year = date.today().year
    mode = normalize_stats_mode(request.args.get("mode", "sales"))
    dimension = normalize_stats_dimension(request.args.get("dimension", "department"))
    if dimension == "user":
        fetch_rows = (
            fetch_user_parts_daily_totals
            if mode == "parts"
            else fetch_user_daily_totals
        )
    else:
        fetch_rows = (
            fetch_department_parts_daily_totals
            if mode == "parts"
            else fetch_department_daily_totals
        )

    rows = fetch_rows(department, current_year, month)

    labels = []
    values = []
    for day, total in rows:
        labels.append(str(int(day)))
        values.append(float(total))

    return jsonify({"labels": labels, "values": values, "year": current_year, "month": month})

@app.route("/stats/exclusions", methods=["POST"])
def save_stats_exclusions():
    filter_type = request.form.get("filter", "this_month")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    mode = request.form.get("mode", "sales")
    dimension = request.form.get("dimension", "department")
    excluded_departments = request.form.getlist("exclude")

    if normalize_stats_dimension(dimension) == "user":
        session["stats_excluded_users"] = excluded_departments
    else:
        session["stats_excluded_departments"] = excluded_departments

    return redirect(
        url_for(
            "stats",
            filter=filter_type,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            dimension=dimension,
            exclude=excluded_departments,
        )
    )
    
@app.route("/logs/download")
def download_logs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT to_char(timestamp, 'DD.MM.YYYY HH24:MI:SS') AS timestamp, username, action, regnumber, stocknumber, vstockno, location, status
        FROM public.hpd3281
        ORDER BY timestamp DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    import pandas as pd
    from io import BytesIO
    output = BytesIO()
    df = pd.DataFrame(rows, columns=["Timestamp", "User", "Action", "Registration", "Stock Number", "vStockNo", "Location", "Status"])
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Logs")
    output.seek(0)

    return send_file(output, download_name="logs.xlsx", as_attachment=True)



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
