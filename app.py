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
from typing import List, Tuple, Optional
from calendar import monthrange
import json
import os
from urllib.parse import urljoin
import pyodbc


def rgb_to_hex(rgb):
    r = int(rgb.get('red', 1) * 255)
    g = int(rgb.get('green', 1) * 255)
    b = int(rgb.get('blue', 1) * 255)
    return '#{:02X}{:02X}{:02X}'.format(r, g, b)
    
def _load_json_file(path: str, fallback):
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def _normalise_order_store(raw_data) -> dict:
    if isinstance(raw_data, list):
        # Legacy format where the order was global for everyone.
        return {"__default__": [str(item) for item in raw_data]}
    if isinstance(raw_data, dict):
        normalised = {}
        for key, value in raw_data.items():
            if isinstance(value, list):
                normalised[str(key)] = [str(item) for item in value]
        return normalised
    return {}


def load_department_order(user: Optional[str] = None) -> List[str]:
    raw_data = _load_json_file(DEPARTMENT_ORDER_PATH, {})
    store = _normalise_order_store(raw_data)
    if user and user in store:
        return store[user]
    return store.get("__default__", [])


def persist_department_order(order: List[str], user: Optional[str] = None) -> None:
    raw_data = _load_json_file(DEPARTMENT_ORDER_PATH, {})
    store = _normalise_order_store(raw_data)
    key = user or "__default__"
    store[key] = [str(item) for item in order]
    with open(DEPARTMENT_ORDER_PATH, "w", encoding="utf-8") as f:
        json.dump(store, f)


def _normalise_exclusion_store(raw_data) -> dict:
    if not isinstance(raw_data, dict):
        return {}
    normalised = {}
    for user, payload in raw_data.items():
        if isinstance(payload, dict):
            normalised[user] = {
                "department": [str(item) for item in payload.get("department", [])],
                "user": [str(item) for item in payload.get("user", [])],
                "insurance_company": [
                    str(item) for item in payload.get("insurance_company", [])
                ],
            }
    return normalised


def format_inventorylog_details(details: Optional[str]) -> str:
    if not details:
        return ""
    replacements = {
        "[LocaleResources:inventoryLogMessage.changedFrom]": "From",
        "[inventoryLogMessage.changedFrom]": "From",
        "[LocaleResources:inventoryLogMessage.changedTo]": "To",
        "[inventoryLogMessage.changedTo]": "To",
    }
    for token, replacement in replacements.items():
        details = details.replace(token, replacement)
    return details


def load_stats_exclusions(user: Optional[str], dimension: str) -> List[str]:
    raw_data = _load_json_file(STATS_EXCLUSIONS_PATH, {})
    store = _normalise_exclusion_store(raw_data)
    key = user or "__default__"
    return store.get(key, {}).get(dimension, [])


def persist_stats_exclusions(user: Optional[str], dimension: str, exclusions: List[str]) -> None:
    raw_data = _load_json_file(STATS_EXCLUSIONS_PATH, {})
    store = _normalise_exclusion_store(raw_data)
    key = user or "__default__"
    user_entry = store.setdefault(
        key, {"department": [], "user": [], "insurance_company": []}
    )
    user_entry[dimension] = [str(item) for item in exclusions]
    with open(STATS_EXCLUSIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(store, f)


def fetch_auction_slides() -> Tuple[List[dict], Optional[str]]:
    try:
        response = requests.get(
            AUCTIONS_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        response.raise_for_status()
    except Exception as exc:
        return [], f"Unable to load auctions right now: {exc}"

    soup = BeautifulSoup(response.text, "html.parser")
    slides = []
    for img in soup.select("img"):
        src = img.get("data-src") or img.get("data-original") or img.get("src")
        if not src or src.startswith("data:"):
            continue
        slides.append(
            {
                "src": urljoin(AUCTIONS_URL, src),
                "title": img.get("alt") or "Auction listing",
            }
        )
        if len(slides) >= 20:
            break
    if not slides:
        return [], "No auction images were found in the search results."
    return slides, None

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
STATS_EXCLUSIONS_PATH = os.path.join(BASE_DIR, "stats_exclusions.json")
AUCTIONS_URL = "https://www.salvagemarket.co.uk/Search?auction[]=&bucketDetails=&bucketId=&damageCategory[]=&distance[]=&editorPickSearch=0&freeSubscriptionOnly=false&fuelType[]=&latitude=0&longitude=0&make[]=&model[]=&orderBy=1&pageNumber=0&pageSize=20&quickSearch=0&searchText=&seller[]=ca35a24f-c044-420d-9c1b-9aa05beb8e96&startDrive[]=&transmissionType[]=&year[]="

@app.context_processor
def inject_current_user():
    return {"current_user": session.get("username")}

USERS = {
    'admin': 'Silverlake1!',
    'paul': 'Silverlake1!',
    'morgan': 'Silverlake1!',
    'cain': 'Silverlake1!',
    'stores': 'stores',
    'Stores': 'stores',
    'Josh': 'Silverlake1!',
    'Casper': 'Silverlake1!',
    'carlo': 'Silverlake1!',
    'nacho': 'Silverlake1!'
}

last_search_result = None
search_details = None

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    next_url = request.args.get('next') or request.form.get('next')
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in USERS and USERS[username] == password:
            session['logged_in'] = True
            session['login_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            session['username'] = username
            if next_url:
                return redirect(next_url)
            return redirect(url_for('index'))
        else:
            error = 'Invalid Credentials. Please try again.'
    return render_template('login.html', error=error, next_url=next_url)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.before_request
def require_login():
    allowed_routes = ['login', 'static', 'autocomplete_model']
    if request.endpoint not in allowed_routes and not session.get('logged_in'):
        next_url = request.url
        return redirect(url_for('login', next=next_url))
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


@app.route("/auctions", methods=["GET"])
def auctions():
    slides, error = fetch_auction_slides()
    return render_template(
        "auctions.html",
        slides=slides,
        error=error,
        source_url=AUCTIONS_URL,
    )

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
IMAGE_BASE_URL = "http://192.168.10.23/pinproHostedImages/"

ATLAS_DB_HOST = os.getenv("ATLAS_DB_HOST", "52.51.93.215")
ATLAS_DB_PORT = int(os.getenv("ATLAS_DB_PORT", "1433"))
ATLAS_DB_NAME = os.getenv("ATLAS_DB_NAME", "silverlake")
ATLAS_DB_NAMES = os.getenv("ATLAS_DB_NAMES", "")
ATLAS_DB_USER = os.getenv("ATLAS_DB_USER", "nacho")
ATLAS_DB_PASSWORD = os.getenv("ATLAS_DB_PASSWORD", "Merry32Nacho58")
ATLAS_DB_DRIVER = os.getenv("ATLAS_DB_DRIVER", "ODBC Driver 18 for SQL Server")
ATLAS_DB_ENCRYPT = os.getenv("ATLAS_DB_ENCRYPT", "yes")
ATLAS_DB_TRUST_CERT = os.getenv("ATLAS_DB_TRUST_CERT", "yes")


def normalize_image_url(relative_url: Optional[str]) -> Optional[str]:
    if not relative_url:
        return None
    cleaned_relative = str(relative_url).lstrip("/")
    if cleaned_relative.lower().startswith("http"):
        return cleaned_relative
    return f"{IMAGE_BASE_URL.rstrip('/')}/{cleaned_relative}"

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


def _get_atlas_db_name_candidates() -> List[str]:
    explicit_names = [name.strip() for name in ATLAS_DB_NAMES.split(",") if name.strip()]
    if ATLAS_DB_NAME:
        return [ATLAS_DB_NAME, *explicit_names]
    return explicit_names


def get_atlas_db_connection(database_name: str):
    conn_str = (
        f"DRIVER={{{ATLAS_DB_DRIVER}}};"
        f"SERVER={ATLAS_DB_HOST},{ATLAS_DB_PORT};"
        f"DATABASE={database_name};"
        f"UID={ATLAS_DB_USER};"
        f"PWD={ATLAS_DB_PASSWORD};"
        f"Encrypt={ATLAS_DB_ENCRYPT};"
        f"TrustServerCertificate={ATLAS_DB_TRUST_CERT};"
    )
    return pyodbc.connect(conn_str, timeout=150)


def _fetch_table_columns(cursor, full_table_name: str) -> List[Tuple[str, str]]:
    cursor.execute(
        """
        SELECT c.name, t.name
        FROM sys.columns AS c
        JOIN sys.types AS t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(?)
        ORDER BY c.column_id
        """,
        (full_table_name,),
    )
    return cursor.fetchall()


def _build_select_list(column_types: List[Tuple[str, str]]) -> List[str]:
    select_columns = []
    for column_name, type_name in column_types:
        if type_name.lower() == "datetimeoffset":
            select_columns.append(
                f"CAST([{column_name}] AS datetime2) AS [{column_name}]"
            )
        else:
            select_columns.append(f"[{column_name}]")
    return select_columns


def fetch_atlas_vehicle_stats(limit: int = 1000):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = """
                SELECT
                    v.Id,
                    v.RegNo AS "Registration",
                    CAST(v.DateEntered AS datetime2) AS DateEntered,
                    m.Name AS Manufacturer,
                    mg.Name AS Model,
                    dd.TrimLevel,
                    col.Name AS colour,
                    dm.Name AS Derivative,
                    ib.Name AS InsuranceBranch,
                    ic.Name AS InsuranceCompany,
                    c.Code AS Category_Code,
                    c.Name AS Category,
                    CAST(v.DateRecoveredStart AS datetime2) AS "Date Recovered START",
                    CAST(v.DateRecoveredEnd AS datetime2) AS "Date Recovered END",
                    CAST(sr.DateRecovered AS datetime2) AS "Date Recovered",
                    CAST(sc.DateCleared AS datetime2) AS DateCleared,
                    CAST(scn.DateCancelled AS datetime2) AS DateCancelled,
                    CAST(ss.DateSold AS datetime2) AS DateSold,
                    ss.IncVAT AS Sold_price,
                    stc.Name AS "Status"
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                LEFT JOIN PartDataManufacturers m ON v.ManufacturerId = m.Id
                LEFT JOIN PartDataModelGroups mg ON v.ModelGroupId = mg.Id
                LEFT JOIN PartDataDerivativeDetails dd ON v.DerivativeId = dd.Id
                LEFT JOIN PartDataModels dm ON v.DerivativeId = dm.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                LEFT JOIN Categories c ON v.CategoryId = c.Id
                LEFT JOIN SalvageClears sc ON v.Id = sc.CtVehicleId
                LEFT JOIN SalvagesCancelled scn ON v.Id = scn.CtVehicleId
                LEFT JOIN SalvageSales ss ON v.Id = ss.CtVehicleId
                LEFT JOIN SalvageRecoveries srec ON v.Id = srec.Id
                LEFT JOIN PartDataColours col ON v.ColourId = col.Id
                LEFT JOIN StatusColors stc ON stc.Status = v.StatusEnum
                WHERE CAST(sr.DateRecovered AS datetime2) >= '2026-01-24'
                AND CAST(sr.DateRecovered AS datetime2) < '2026-01-24'
                ORDER BY v.Id DESC
            """
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            return database_name, columns, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_counts_by_insurance(start_date: date, end_date: date):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = """
                SELECT
                    ic.Name,
                    COUNT(*) AS VehicleCount
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                WHERE CAST(sr.DateRecovered AS datetime2) >= ?
                  AND CAST(sr.DateRecovered AS datetime2) < ?
                GROUP BY ic.Name
                ORDER BY VehicleCount DESC, ic.Name
            """
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return database_name, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_counts_by_contract_group(start_date: date, end_date: date):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = """
                SELECT
                    COALESCE(cg.Name, 'Unassigned') AS ContractGroup,
                    ic.Name AS InsuranceCompany,
                    COUNT(*) AS VehicleCount
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                LEFT JOIN ContractGroups cg ON ic.ContractGroupId = cg.Id
                WHERE CAST(sr.DateRecovered AS datetime2) >= ?
                  AND CAST(sr.DateRecovered AS datetime2) < ?
                GROUP BY COALESCE(cg.Name, 'Unassigned'), ic.Name
                ORDER BY VehicleCount DESC, ContractGroup
            """
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return database_name, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_details_by_insurance(start_date: date, end_date: date):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = """
                SELECT
                    v.Id,
                    v.RegNo AS Registration,
                    CAST(v.DateEntered AS datetime2) AS DateEntered,
                    m.Name AS Manufacturer,
                    mg.Name AS Model,
                    dd.TrimLevel,
                    col.Name AS colour,
                    dm.Name AS Derivative,
                    ib.Name AS InsuranceBranch,
                    ic.Name AS InsuranceCompany,
                    c.Code AS Category_Code,
                    c.Name AS Category,
                    CAST(v.DateRecoveredStart AS datetime2) AS [Date Recovered START],
                    CAST(v.DateRecoveredEnd AS datetime2) AS [Date Recovered END],
                    CAST(sr.DateRecovered AS datetime2) AS [Date Recovered],
                    CAST(sc.DateCleared AS datetime2) AS DateCleared,
                    CAST(scn.DateCancelled AS datetime2) AS DateCancelled,
                    CAST(ss.DateSold AS datetime2) AS DateSold,
                    ss.IncVAT AS Sold_price,
                    stc.Name AS Status
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                LEFT JOIN PartDataManufacturers m ON v.ManufacturerId = m.Id
                LEFT JOIN PartDataModelGroups mg ON v.ModelGroupId = mg.Id
                LEFT JOIN PartDataDerivativeDetails dd ON v.DerivativeId = dd.Id
                LEFT JOIN PartDataModels dm ON v.DerivativeId = dm.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                LEFT JOIN Categories c ON v.CategoryId = c.Id
                OUTER APPLY (
                    SELECT TOP (1) sc.DateCleared
                    FROM SalvageClears sc
                    WHERE sc.CtVehicleId = v.Id
                    ORDER BY sc.DateCleared DESC
                ) sc
                OUTER APPLY (
                    SELECT TOP (1) scn.DateCancelled
                    FROM SalvagesCancelled scn
                    WHERE scn.CtVehicleId = v.Id
                    ORDER BY scn.DateCancelled DESC
                ) scn
                OUTER APPLY (
                    SELECT TOP (1) ss.DateSold, ss.IncVAT
                    FROM SalvageSales ss
                    WHERE ss.CtVehicleId = v.Id
                    ORDER BY ss.DateSold DESC
                ) ss
                LEFT JOIN PartDataColours col ON v.ColourId = col.Id
                LEFT JOIN StatusColors stc ON v.StatusEnum = stc.Id
                WHERE CAST(sr.DateRecovered AS datetime2) >= ?
                  AND CAST(sr.DateRecovered AS datetime2) < ?
                ORDER BY v.Id DESC
            """
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            return database_name, columns, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_table_samples(limit: int = 20):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT s.name, t.name
                FROM sys.tables AS t
                JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                ORDER BY s.name, t.name
                """
            )
            tables = cur.fetchall()
            safe_limit = int(limit)
            table_samples = []
            for schema_name, table_name in tables:
                full_table_name = f"{schema_name}.{table_name}"
                try:
                    column_types = _fetch_table_columns(cur, full_table_name)
                    if not column_types:
                        table_samples.append(
                            {
                                "schema": schema_name,
                                "table": table_name,
                                "columns": [],
                                "rows": [],
                                "error": "No columns found.",
                            }
                        )
                        continue
                    select_list = ", ".join(_build_select_list(column_types))
                    cur.execute(
                        f"SELECT TOP ({safe_limit}) {select_list} "
                        f"FROM [{schema_name}].[{table_name}]"
                    )
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    table_samples.append(
                        {
                            "schema": schema_name,
                            "table": table_name,
                            "columns": columns,
                            "rows": rows,
                            "error": None,
                        }
                    )
                except Exception as exc:
                    table_samples.append(
                        {
                            "schema": schema_name,
                            "table": table_name,
                            "columns": [],
                            "rows": [],
                            "error": str(exc),
                        }
                    )
            cur.close()
            conn.close()
            return database_name, table_samples
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")

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


def fetch_images_by_barcode(barcode: str) -> List[Tuple[str, Optional[int]]]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT relativeurl, displayorder
            FROM image
            WHERE barcode = %s
              AND COALESCE(thumbnail, false) = false
            ORDER BY COALESCE(displayorder, 0), relativeurl
            """,
            (barcode,),
        )
        rows = cur.fetchall()
        rows = [
            (relativeurl, displayorder)
            for relativeurl, displayorder in rows
            if relativeurl and not str(relativeurl).lower().startswith("fto/")
        ]
        if rows:
            return rows

        if not barcode.startswith("2-"):
            return []

        trimmed_tag = barcode[2:]
        cur.execute(
            """
            SELECT invnumber
            FROM inventory
            WHERE tag = %s
            ORDER BY invnumber
            LIMIT 1
            """,
            (trimmed_tag,),
        )
        invnumber_row = cur.fetchone()
        if not invnumber_row:
            cur.execute(
                """
                SELECT invnumber
                FROM sold
                WHERE tag = %s
                ORDER BY invnumber
                LIMIT 1
                """,
                (trimmed_tag,),
            )
            invnumber_row = cur.fetchone()
        if not invnumber_row:
            return []

        invnumber = invnumber_row[0]
        cur.execute(
            """
            SELECT relativeurl, displayorder
            FROM image
            WHERE invnumber = %s
              AND COALESCE(thumbnail, false) = false
            ORDER BY COALESCE(displayorder, 0), relativeurl
            """,
            (invnumber,),
        )
        rows = cur.fetchall()
        return [
            (relativeurl, displayorder)
            for relativeurl, displayorder in rows
            if relativeurl and not str(relativeurl).lower().startswith("fto/")
        ]
    finally:
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


def fetch_user_images(start_date: date, end_date: date) -> List[Tuple[str, int]]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(us.shortname, 'Unknown') AS shortname,
               COUNT(invl.invnumber) AS images
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.type_id = '902'
          AND invl.created >= %s
          AND invl.created < %s
        GROUP BY shortname
        ORDER BY shortname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [(row[0], int(row[1])) for row in rows]
  
  
def fetch_user_parts_imaged(start_date: date, end_date: date) -> List[Tuple[str, int]]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(us.shortname, 'Unknown') AS shortname,
               COUNT(DISTINCT invl.invnumber) AS parts_imaged
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.type_id = '902'
          AND invl.created >= %s
          AND invl.created < %s
        GROUP BY shortname
        ORDER BY shortname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [(row[0], int(row[1])) for row in rows]


def fetch_image_timeline(start_date: date, end_date: date) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (invl.invnumber)
            COALESCE(us.shortname, 'Unknown') AS shortname,
            invl.invnumber,
            invl.created,
            thumb.relativeurl AS thumb_url,
            fullimg.full_urls AS full_urls,
            COALESCE(inv.tag, sold.tag, '') AS tag,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM sold s
                    WHERE s.invnumber = invl.invnumber
                      AND s.issold
                ) THEN 'sold'
                WHEN EXISTS (
                    SELECT 1
                    FROM sold s
                    WHERE s.invnumber = invl.invnumber
                      AND NOT s.issold
                ) THEN 'pending'
                WHEN EXISTS (
                    SELECT 1
                    FROM inventory i
                    WHERE i.invnumber = invl.invnumber
                ) THEN 'inventory'
                ELSE 'inventory'
            END AS status
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        LEFT JOIN inventory inv ON inv.invnumber = invl.invnumber
        LEFT JOIN sold ON sold.invnumber = invl.invnumber
        LEFT JOIN LATERAL (
            SELECT relativeurl
            FROM image
            WHERE invnumber = invl.invnumber
              AND COALESCE(thumbnail, false) = true
            ORDER BY COALESCE(displayorder, 0), relativeurl
            LIMIT 1
        ) thumb ON true
        LEFT JOIN LATERAL (
            SELECT ARRAY_AGG(relativeurl ORDER BY COALESCE(displayorder, 0), relativeurl) AS full_urls
            FROM image
            WHERE invnumber = invl.invnumber
              AND COALESCE(thumbnail, false) = false
        ) fullimg ON true
        WHERE invl.type_id = '902'
          AND invl.created >= %s
          AND invl.created < %s
          AND invl.created::time >= TIME '06:00'
          AND invl.created::time <= TIME '18:00'
        ORDER BY invl.invnumber, invl.created
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    timeline = []
    for shortname, invnumber, created, thumb_url, full_urls, tag, status in rows:
        full_urls = full_urls or []
        cleaned_full_urls = [
            normalize_image_url(url)
            for url in full_urls
            if url and not str(url).lower().startswith("fto/")
        ]
        if not thumb_url:
            normalized_thumb = cleaned_full_urls[0] if cleaned_full_urls else None
        elif str(thumb_url).lower().startswith("fto/"):
            normalized_thumb = cleaned_full_urls[0] if cleaned_full_urls else None
        else:
            normalized_thumb = normalize_image_url(thumb_url)
        if not normalized_thumb:
            continue
        preview_urls = cleaned_full_urls[:5] if cleaned_full_urls else [normalized_thumb]
        timeline.append(
            {
                "user": shortname,
                "invnumber": invnumber,
                "created": created,
                "thumb_url": normalized_thumb,
                "full_urls": cleaned_full_urls or [normalized_thumb],
                "preview_urls": preview_urls,
                "tag": tag,
                "status": status,
            }
        )
    return timeline


def fetch_stores_timeline(start_date: date, end_date: date) -> List[dict]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COALESCE(us.shortname, 'Unknown') AS shortname,
            invl.invnumber,
            invl.created,
            invl.details,
            thumb.relativeurl AS thumb_url,
            fullimg.full_urls AS full_urls,
            COALESCE(inv.tag, sold.tag, '') AS tag,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM sold s
                    WHERE s.invnumber = invl.invnumber
                      AND s.issold
                ) THEN 'sold'
                WHEN EXISTS (
                    SELECT 1
                    FROM sold s
                    WHERE s.invnumber = invl.invnumber
                      AND NOT s.issold
                ) THEN 'pending'
                WHEN EXISTS (
                    SELECT 1
                    FROM inventory i
                    WHERE i.invnumber = invl.invnumber
                ) THEN 'inventory'
                ELSE 'inventory'
            END AS status
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        LEFT JOIN inventory inv ON inv.invnumber = invl.invnumber
        LEFT JOIN sold ON sold.invnumber = invl.invnumber
        LEFT JOIN LATERAL (
            SELECT relativeurl
            FROM image
            WHERE invnumber = invl.invnumber
              AND COALESCE(thumbnail, false) = true
            ORDER BY COALESCE(displayorder, 0), relativeurl
            LIMIT 1
        ) thumb ON true
        LEFT JOIN LATERAL (
            SELECT ARRAY_AGG(relativeurl ORDER BY COALESCE(displayorder, 0), relativeurl) AS full_urls
            FROM image
            WHERE invnumber = invl.invnumber
              AND COALESCE(thumbnail, false) = false
        ) fullimg ON true
        WHERE invl.type_id = '442'
          AND invl.created >= %s
          AND invl.created < %s
          AND invl.created::time >= TIME '06:00'
          AND invl.created::time <= TIME '18:00'
        ORDER BY invl.created
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    timeline = []
    for shortname, invnumber, created, details, thumb_url, full_urls, tag, status in rows:
        full_urls = full_urls or []
        cleaned_full_urls = [
            normalize_image_url(url)
            for url in full_urls
            if url and not str(url).lower().startswith("fto/")
        ]
        if not thumb_url:
            normalized_thumb = cleaned_full_urls[0] if cleaned_full_urls else None
        elif str(thumb_url).lower().startswith("fto/"):
            normalized_thumb = cleaned_full_urls[0] if cleaned_full_urls else None
        else:
            normalized_thumb = normalize_image_url(thumb_url)
        if not normalized_thumb:
            continue
        timeline.append(
            {
                "user": shortname,
                "invnumber": invnumber,
                "created": created,
                "thumb_url": normalized_thumb,
                "full_urls": cleaned_full_urls or [normalized_thumb],
                "tag": tag,
                "status": status,
                "details": format_inventorylog_details(details),
            }
        )
    return timeline


def fetch_timeline_users(start_date: date, end_date: date) -> List[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT COALESCE(us.shortname, 'Unknown') AS shortname
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.type_id = '902'
          AND invl.created >= %s
          AND invl.created < %s
        ORDER BY shortname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]


def fetch_stores_timeline_users(start_date: date, end_date: date) -> List[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT COALESCE(us.shortname, 'Unknown') AS shortname
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.type_id = '442'
          AND invl.created >= %s
          AND invl.created < %s
        ORDER BY shortname
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]

def fetch_parts_breakdown(
    entity_value: str, start_date: date, end_date: date, dimension: str
) -> List[Tuple[str, int]]:
    """Return itemname counts for the given department or user."""

    conn = get_db_connection()
    cur = conn.cursor()

    dimension_key = "user" if normalize_stats_dimension(dimension) == "user" else "department"
    joins = """
        LEFT JOIN invoice inv ON inv.invoice_id = sold.invoice_id
        LEFT JOIN itemtype it ON it.itemtype_id = sold.itemtype_id
    """
    filter_clause = "COALESCE(inv.departmentname, 'Unknown') = %s"
    params = [start_date, end_date, entity_value]

    if dimension_key == "user":
        joins += " LEFT JOIN pinuser us ON us.user_id = inv.whocreated_id"
        filter_clause = "COALESCE(us.shortname, 'Unknown') = %s"

    cur.execute(
        f"""
        SELECT
            COALESCE(REPLACE(REPLACE(REPLACE(it.itemname, '[', ''), ']', ''), '_', ' '), 'Unknown') AS itemname,
            COUNT(sold.invnumber) AS parts_sold
        FROM sold
        {joins}
        WHERE sold.issold
          AND solddate >= %s
          AND solddate < %s
          AND {filter_clause}
        GROUP BY itemname
        ORDER BY parts_sold DESC, itemname
        """,
        params,
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [(row[0], int(row[1])) for row in rows]

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

def fetch_user_images_monthly_totals(user: str, year: int) -> List[Tuple[int, int]]:
    start_year = date(year, 1, 1)
    start_next_year = date(year + 1, 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(MONTH FROM invl.created)::int AS month,
               COUNT(invl.invnumber) AS images
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.created >= %s
          AND invl.created < %s
          AND invl.type_id = '902'
          AND COALESCE(us.shortname, 'Unknown') = %s
        GROUP BY month
        ORDER BY month
        """,
        (start_year, start_next_year, user),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_user_parts_imaged_monthly_totals(user: str, year: int) -> List[Tuple[int, int]]:
    start_year = date(year, 1, 1)
    start_next_year = date(year + 1, 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(MONTH FROM invl.created)::int AS month,
               COUNT(DISTINCT invl.invnumber) AS parts_imaged
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.created >= %s
          AND invl.created < %s
          AND invl.type_id = '902'
          AND COALESCE(us.shortname, 'Unknown') = %s
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


def fetch_user_parts_monthly_totals(user: str, year: int) -> List[Tuple[int, float]]:
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
        LEFT JOIN pinuser us ON us.user_id = inv.whocreated_id
        WHERE solddate >= %s
          AND solddate < %s
          AND us.shortname = %s
          AND sold.issold
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


def fetch_user_images_daily_totals(user: str, year: int, month: int) -> List[Tuple[int, int]]:
    start_month = date(year, month, 1)
    if month == 12:
        start_next_month = date(year + 1, 1, 1)
    else:
        start_next_month = date(year, month + 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(DAY FROM invl.created)::int AS day,
               COUNT(invl.invnumber) AS images
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.created >= %s
          AND invl.created < %s
          AND invl.type_id = '902'
          AND COALESCE(us.shortname, 'Unknown') = %s
        GROUP BY day
        ORDER BY day
        """,
        (start_month, start_next_month, user),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_user_parts_imaged_daily_totals(user: str, year: int, month: int) -> List[Tuple[int, int]]:
    start_month = date(year, month, 1)
    if month == 12:
        start_next_month = date(year + 1, 1, 1)
    else:
        start_next_month = date(year, month + 1, 1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXTRACT(DAY FROM invl.created)::int AS day,
               COUNT(DISTINCT invl.invnumber) AS parts_imaged
        FROM inventorylog invl
        LEFT JOIN pinuser us ON us.user_id = invl.user_id
        WHERE invl.created >= %s
          AND invl.created < %s
          AND invl.type_id = '902'
          AND COALESCE(us.shortname, 'Unknown') = %s
        GROUP BY day
        ORDER BY day
        """,
        (start_month, start_next_month, user),
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


def fetch_user_parts_daily_totals(user: str, year: int, month: int) -> List[Tuple[int, float]]:
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
        LEFT JOIN pinuser us ON us.user_id = inv.whocreated_id
        WHERE solddate >= %s
          AND solddate < %s
          AND us.shortname = %s
          AND sold.issold
        GROUP BY day
        ORDER BY day
        """,
        (start_month, start_next_month, user),
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
    if filter_type == "this_week":
        start_of_week = today - timedelta(days=today.weekday())
        return start_of_week, start_of_week + timedelta(days=7)
    if filter_type == "last_week":
        start_of_week = today - timedelta(days=today.weekday())
        last_week_start = start_of_week - timedelta(days=7)
        return last_week_start, start_of_week
    if filter_type == "this_month":
        return today.replace(day=1), (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    if filter_type == "last_month":
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - timedelta(days=1)
        return last_month_end.replace(day=1), first_this_month
    if filter_type == "this_year":
        return date(today.year, 1, 1), date(today.year + 1, 1, 1)
    if filter_type == "last_year":
        return date(today.year - 1, 1, 1), date(today.year, 1, 1)
    if filter_type == "custom" and start_date_str and end_date_str:
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str) + timedelta(days=1)
        return start_date, end_date

    return today, today + timedelta(days=1)
    
    
def describe_date_range(filter_type: str, start_date: date, end_date: date) -> str:
    if not start_date or not end_date:
        return "All Time"

    labels = {
        "today": "Today",
        "yesterday": "Yesterday",
        "this_week": "This Week",
        "last_week": "Last Week",
        "this_month": "This Month",
        "last_month": "Last Month",
        "this_year": "This Year",
        "last_year": "Last Year",
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


@app.route("/image_lookup", methods=["GET", "POST"])
def image_lookup():
    barcode_query = ""
    form_barcode_value = ""
    images = []
    error_message = None

    if request.method == "POST":
        barcode_query = request.form.get("barcode", "").strip()
    else:
        barcode_query = request.args.get("barcode", "").strip()

    if barcode_query:
        try:
            rows = fetch_images_by_barcode(barcode_query)
            images = []
            for relative_url, display_order in rows:
                if relative_url.startswith(("http://", "https://")):
                    final_url = relative_url
                else:
                    cleaned_relative = relative_url.lstrip("/")
                    final_url = f"{IMAGE_BASE_URL.rstrip('/')}/{cleaned_relative}"
                images.append({"url": final_url, "displayorder": display_order})
            if not images:
                error_message = f"No images found for tag {barcode_query}."
        except Exception as exc:
            error_message = "Something went wrong while searching for images."
            print(f"Error fetching images for barcode {barcode_query}: {exc}")
    elif request.method == "POST":
        error_message = "Please enter a tag number to search."

    return render_template(
        "image_lookup.html",
        barcode_query=barcode_query,
        images=images,
        error_message=error_message,
        active_page="image_lookup",
    )


def normalize_stats_mode(mode: str) -> str:
    return "parts" if str(mode).lower() == "parts" else "sales"


def normalize_stats_dimension(dimension: str) -> str:
    return "user" if str(dimension).lower() == "user" else "department"


def normalize_image_stats_mode(mode: str) -> str:
    normalized = str(mode).lower()
    if normalized in {"parts", "parts_imaged", "parts-imaged", "partsimaged"}:
        return "parts_imaged"
    return "images"


def normalize_prev_period_mode(mode: str) -> str:
    return "month" if str(mode).lower() == "month" else "mirror"


def shift_one_year_back(value: date) -> date:
    """Return the same calendar day in the previous year, clamped for leap days."""

    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        return value.replace(year=value.year - 1, day=28)


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
    current_user = session.get("username")
    saved_order = load_department_order(current_user)
    order_index = {name: idx for idx, name in enumerate(saved_order)}

    default_exclusions = load_stats_exclusions(
        current_user, "department" if resolved_dimension == "department" else "user"
    )
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
        "entity_label": "User",
        "entity_label_plural": "Users",
    }


def build_vehicle_stats_context(
    filter_type: str,
    start_date_str: str,
    end_date_str: str,
    exclude_args: List[str],
    group_mode: str,
):
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)

    resolved_group_mode = "contract" if group_mode == "contract" else "company"
    if resolved_group_mode == "contract":
        database_name, rows = fetch_atlas_vehicle_counts_by_contract_group(
            start_date, end_date
        )
    else:
        database_name, rows = fetch_atlas_vehicle_counts_by_insurance(start_date, end_date)
    details_db_name, detail_columns, detail_rows = fetch_atlas_vehicle_details_by_insurance(
        start_date, end_date
    )
    current_user = session.get("username")
    default_exclusions = load_stats_exclusions(current_user, "insurance_company")
    excluded_companies = exclude_args or default_exclusions

    if resolved_group_mode == "contract":
        group_totals: dict[str, int] = {}
        all_companies = sorted({row[1] for row in rows})
        for contract_group, company, count in rows:
            if company in excluded_companies:
                continue
            group_totals[contract_group] = group_totals.get(contract_group, 0) + int(
                count
            )
        filtered_rows = sorted(
            group_totals.items(),
            key=lambda item: (-item[1], item[0]),
        )
        entity_label = "Contract Group"
    else:
        filtered_rows = [
            (row[0], int(row[1]))
            for row in rows
            if row[0] not in excluded_companies
        ]
        all_companies = sorted({row[0] for row in rows})
        entity_label = "Insurance Company"

    sum_total = sum(row[1] for row in filtered_rows)

    chart_labels = [row[0] for row in filtered_rows]
    chart_values = [float(row[1]) for row in filtered_rows]
    chart_title_base = f"Vehicles by {entity_label}"

    return {
        "filter_type": filter_type,
        "start_date": start_date,
        "end_date": end_date,
        "date_range_label": date_range_label,
        "rows": filtered_rows,
        "sum_total": sum_total,
        "chart_labels": chart_labels,
        "chart_values": chart_values,
        "all_companies": all_companies,
        "excluded_companies": excluded_companies,
        "database_name": database_name or details_db_name,
        "detail_columns": detail_columns,
        "detail_rows": detail_rows,
        "group_mode": resolved_group_mode,
        "entity_label": entity_label,
        "chart_title_base": chart_title_base,
    }


def build_image_stats_context(
    filter_type: str,
    start_date_str: str,
    end_date_str: str,
    exclude_args: List[str],
    mode: str,
    prev_mode: str,
):
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)

    resolved_mode = normalize_image_stats_mode(mode)
    mode_label = "Parts Imaged" if resolved_mode == "parts_imaged" else "Images"
    value_format = "count"
    value_label = mode_label
    value_vat_label = f"{mode_label} (Prev Period)"

    fetch_rows = (
        fetch_user_parts_imaged if resolved_mode == "parts_imaged" else fetch_user_images
    )

    rows = fetch_rows(start_date, end_date)
    resolved_prev_mode = normalize_prev_period_mode(prev_mode)
    if resolved_prev_mode == "month":
        inclusive_end = end_date - timedelta(days=1)
        prev_start = shift_one_month_back(start_date)
        prev_inclusive_end = shift_one_month_back(inclusive_end)
        prev_end = prev_inclusive_end + timedelta(days=1)
    else:
        range_delta = end_date - start_date
        prev_end = start_date
        prev_start = start_date - range_delta
        prev_inclusive_end = prev_end - timedelta(days=1)
    prev_date_range_label = (
        f"Prev Period ({prev_start.strftime('%d/%m/%Y')} - {prev_inclusive_end.strftime('%d/%m/%Y')})"
        if prev_start != prev_inclusive_end
        else f"Prev Period ({prev_start.strftime('%d/%m/%Y')})"
    )
    prev_rows = fetch_rows(prev_start, prev_end)
    prev_row_map = {row[0]: float(row[1]) for row in prev_rows}
    current_user = session.get("username")
    saved_order = load_department_order(current_user)
    order_index = {name: idx for idx, name in enumerate(saved_order)}

    default_exclusions = load_stats_exclusions(current_user, "user")
    excluded_departments = exclude_args or default_exclusions
    filtered_rows = []
    for row in rows:
        if row[0] in excluded_departments:
            continue
        prev_value = prev_row_map.get(row[0], 0.0)
        filtered_rows.append((row[0], float(row[1]), float(prev_value)))
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
        "image_mode": resolved_mode,
        "prev_mode": resolved_prev_mode,
        "mode_label": mode_label,
        "value_format": value_format,
        "value_label": value_label,
        "value_vat_label": value_vat_label,
        "prev_date_range_label": prev_date_range_label,
        "entity_label": "User",
        "entity_label_plural": "Users",
    }

def build_image_timeline_context(
    filter_type: str,
    start_date_str: str,
    end_date_str: str,
    exclude_args: List[str],
):
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)

    raw_items = fetch_image_timeline(start_date, end_date)
    current_user = session.get("username")
    default_exclusions = load_stats_exclusions(current_user, "user")
    excluded_users = exclude_args or default_exclusions
    raw_items = [item for item in raw_items if item["user"] not in excluded_users]
    raw_items = sorted(raw_items, key=lambda item: (item["user"], item["created"]))
    all_users = fetch_timeline_users(start_date, end_date)

    grouped = defaultdict(lambda: defaultdict(list))
    for item in raw_items:
        day_key = item["created"].date()
        grouped[item["user"]][day_key].append(item)

    timeline_users = []
    for user in sorted(grouped.keys()):
        user_items = [item for day_items in grouped[user].values() for item in day_items]
        part_count = len({item["invnumber"] for item in user_items})
        image_count = sum(len(item["full_urls"]) for item in user_items)
        days = []
        for day in sorted(grouped[user].keys()):
            bucket_counts = defaultdict(int)
            day_items = []
            for item in grouped[user][day]:
                created = item["created"]
                minutes = created.hour * 60 + created.minute
                start_minutes = 6 * 60
                end_minutes = 18 * 60
                if minutes < start_minutes or minutes > end_minutes:
                    continue
                position = ((minutes - start_minutes) / (end_minutes - start_minutes)) * 100
                bucket = minutes // 5
                stack_index = bucket_counts[bucket]
                bucket_counts[bucket] += 1
                day_items.append(
                    {
                        "thumb_url": item["thumb_url"],
                        "full_urls": item["full_urls"],
                        "preview_urls": item["preview_urls"],
                        "tag": item["tag"],
                        "time_label": created.strftime("%H:%M"),
                        "position": position,
                        "stack_index": stack_index,
                    }
                )
            days.append(
                {
                    "date_label": day.strftime("%d/%m/%Y"),
                    "items": day_items,
                }
            )
        timeline_users.append(
            {
                "user": user,
                "days": days,
                "image_count": image_count,
                "part_count": part_count,
            }
        )

    hours = [hour for hour in range(6, 19)]

    return {
        "filter_type": filter_type,
        "start_date": start_date,
        "end_date": end_date,
        "date_range_label": date_range_label,
        "timeline_users": timeline_users,
        "hours": hours,
        "all_users": all_users,
        "excluded_users": excluded_users,
    }


def build_stores_timeline_context(
    filter_type: str,
    start_date_str: str,
    end_date_str: str,
    exclude_args: List[str],
):
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)

    raw_items = fetch_stores_timeline(start_date, end_date)
    current_user = session.get("username")
    default_exclusions = load_stats_exclusions(current_user, "user")
    excluded_users = exclude_args or default_exclusions
    raw_items = [item for item in raw_items if item["user"] not in excluded_users]
    raw_items = sorted(raw_items, key=lambda item: (item["user"], item["created"]))
    all_users = fetch_stores_timeline_users(start_date, end_date)

    grouped = defaultdict(lambda: defaultdict(list))
    for item in raw_items:
        day_key = item["created"].date()
        grouped[item["user"]][day_key].append(item)

    timeline_users = []
    for user in sorted(grouped.keys()):
        user_items = [item for day_items in grouped[user].values() for item in day_items]
        movement_count = len(user_items)
        part_count = len({item["invnumber"] for item in user_items})
        days = []
        for day in sorted(grouped[user].keys()):
            bucket_counts = defaultdict(int)
            day_items = []
            for item in grouped[user][day]:
                created = item["created"]
                minutes = created.hour * 60 + created.minute + (created.second / 60)
                start_minutes = 6 * 60
                end_minutes = 18 * 60
                if minutes < start_minutes or minutes > end_minutes:
                    continue
                position = ((minutes - start_minutes) / (end_minutes - start_minutes)) * 100
                bucket = created.hour * 60 + created.minute
                stack_index = bucket_counts[bucket]
                bucket_counts[bucket] += 1
                day_items.append(
                    {
                        "thumb_url": item["thumb_url"],
                        "full_urls": item["full_urls"],
                        "tag": item["tag"],
                        "time_label": created.strftime("%H:%M:%S"),
                        "position": position,
                        "stack_index": stack_index,
                        "details": item["details"],
                        "status": item["status"],
                    }
                )
            days.append(
                {
                    "date_label": day.strftime("%d/%m/%Y"),
                    "items": day_items,
                }
            )
        timeline_users.append(
            {
                "user": user,
                "days": days,
                "movement_count": movement_count,
                "part_count": part_count,
            }
        )

    hours = [hour for hour in range(6, 19)]

    return {
        "filter_type": filter_type,
        "start_date": start_date,
        "end_date": end_date,
        "date_range_label": date_range_label,
        "timeline_users": timeline_users,
        "hours": hours,
        "all_users": all_users,
        "excluded_users": excluded_users,
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
        active_page="stats",
    )


@app.route("/vehicle_stats", methods=["GET"])
def vehicle_stats():
    filter_type = request.args.get("filter", "today")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    group_mode = request.args.get("group", "company")
    live_enabled = str(request.args.get("live", "")).lower() in {"1", "true", "yes", "on"}

    error_message = None
    try:
        context = build_vehicle_stats_context(
            filter_type, start_date_str, end_date_str, excluded_args, group_mode
        )
    except Exception as exc:
        start_date, end_date = parse_date_filter(
            filter_type, start_date_str, end_date_str
        )
        entity_label = "Contract Group" if group_mode == "contract" else "Insurance Company"
        context = {
            "filter_type": filter_type,
            "start_date": start_date,
            "end_date": end_date,
            "date_range_label": describe_date_range(filter_type, start_date, end_date),
            "rows": [],
            "sum_total": 0,
            "chart_labels": [],
            "chart_values": [],
            "all_companies": [],
            "excluded_companies": excluded_args,
            "database_name": None,
            "group_mode": group_mode,
            "entity_label": entity_label,
            "chart_title_base": f"Vehicles by {entity_label}",
        }
        error_message = f"Unable to load vehicle stats: {exc}"

    return render_template(
        "vehicle_stats.html",
        **context,
        live_enabled=live_enabled,
        error_message=error_message,
        active_page="vehicle_stats",
    )


def atlas_vehicle_stats():
    error_message = None
    database_name = None
    columns = []
    rows = []
    try:
        database_name, columns, rows = fetch_atlas_vehicle_stats()
    except Exception as exc:
        error_message = f"Unable to load Atlas vehicle stats: {exc}"

    return render_template(
        "atlas_vehicle_stats.html",
        database_name=database_name,
        columns=columns,
        rows=rows,
        error_message=error_message,
        active_page="atlas_vehicle_stats",
    )


def atlas_table_samples():
    error_message = None
    database_name = None
    table_samples = []
    try:
        database_name, table_samples = fetch_atlas_table_samples()
    except Exception as exc:
        error_message = f"Unable to load Atlas table samples: {exc}"

    return render_template(
        "atlas_table_samples.html",
        database_name=database_name,
        table_samples=table_samples,
        error_message=error_message,
        active_page="atlas_table_samples",
    )


if "atlas_vehicle_stats" not in app.view_functions:
    app.add_url_rule("/atlas_vehicle_stats", view_func=atlas_vehicle_stats)

if "atlas_table_samples" not in app.view_functions:
    app.add_url_rule("/atlas_table_samples", view_func=atlas_table_samples)


@app.route("/image_stats", methods=["GET"])
def image_stats():
    filter_type = request.args.get("filter", "this_month")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    mode = request.args.get("mode", "images")
    prev_mode = request.args.get("prev_mode", "mirror")

    context = build_image_stats_context(
        filter_type, start_date_str, end_date_str, excluded_args, mode, prev_mode
    )
    live_enabled = str(request.args.get("live", "")).lower() in {"1", "true", "yes", "on"}

    return render_template(
        "stats.html",
        **context,
        live_enabled=live_enabled,
        active_page="stats",
    )

@app.route("/image_timeline", methods=["GET"])
def image_timeline():
    filter_type = request.args.get("filter", "today")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")

    context = build_image_timeline_context(
        filter_type, start_date_str, end_date_str, excluded_args
    )

    return render_template(
        "image_timeline.html",
        **context,
        active_page="image_timeline",
    )


@app.route("/stores_timeline", methods=["GET"])
def stores_timeline():
    filter_type = request.args.get("filter", "today")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")

    context = build_stores_timeline_context(
        filter_type, start_date_str, end_date_str, excluded_args
    )

    return render_template(
        "stores_timeline.html",
        **context,
        active_page="stores_timeline",
    )


@app.route("/stats/data", methods=["GET"])
def stats_data():
    filter_type = request.args.get("filter", "this month")
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


@app.route("/vehicle_stats/data", methods=["GET"])
def vehicle_stats_data():
    filter_type = request.args.get("filter", "today")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    group_mode = request.args.get("group", "company")

    context = build_vehicle_stats_context(
        filter_type, start_date_str, end_date_str, excluded_args, group_mode
    )

    detail_rows = []
    for row in context.get("detail_rows", []):
        serialized_row = []
        for value in row:
            if value is None:
                serialized_row.append("")
            elif isinstance(value, (datetime, date)):
                serialized_row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                serialized_row.append(value)
        detail_rows.append(serialized_row)

    return jsonify(
        {
            "date_range_label": context["date_range_label"],
            "rows": [
                {"label": row[0], "total": float(row[1])} for row in context["rows"]
            ],
            "sum_total": context["sum_total"],
            "chart_labels": context["chart_labels"],
            "chart_values": context["chart_values"],
            "detail_columns": context.get("detail_columns", []),
            "detail_rows": detail_rows,
            "entity_label": context.get("entity_label", "Insurance Company"),
            "chart_title_base": context.get("chart_title_base", "Vehicles by Insurance Company"),
        }
    )


@app.route("/image_stats/data", methods=["GET"])
def image_stats_data():
    filter_type = request.args.get("filter", "this_month")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    mode = request.args.get("mode", "images")

    context = build_image_stats_context(
        filter_type, start_date_str, end_date_str, excluded_args, mode
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


@app.route("/stats/parts_breakdown", methods=["GET"])
def stats_parts_breakdown():
    mode = normalize_stats_mode(request.args.get("mode", "sales"))
    if mode != "parts":
        return jsonify({"error": "Parts breakdown is only available for Parts mode"}), 400

    filter_type = request.args.get("filter", "this_month")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    dimension = normalize_stats_dimension(request.args.get("dimension", "department"))
    entity_value = request.args.get("entity")

    if not entity_value:
        return jsonify({"error": "Missing entity"}), 400

    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)
    rows = fetch_parts_breakdown(entity_value, start_date, end_date, dimension)
    total = sum(row[1] for row in rows)

    return jsonify(
        {
            "entity": entity_value,
            "dimension": dimension,
            "date_range_label": date_range_label,
            "items": [{"itemname": row[0], "count": row[1]} for row in rows],
            "total": total,
        }
    )


@app.route("/stats/order", methods=["POST"])
def save_department_order():
    data = request.get_json(silent=True) or {}
    order = data.get("order", [])

    if not isinstance(order, list):
        return jsonify({"error": "Invalid order payload"}), 400

    normalized_order = [str(item) for item in order]
    user = session.get("username")
    persist_department_order(normalized_order, user)

    return jsonify({"status": "saved", "order": normalized_order})
    
    
@app.route("/image_stats/order", methods=["POST"])
def save_image_user_order():
    data = request.get_json(silent=True) or {}
    order = data.get("order", [])

    if not isinstance(order, list):
        return jsonify({"error": "Invalid order payload"}), 400

    normalized_order = [str(item) for item in order]
    user = session.get("username")
    persist_department_order(normalized_order, user)

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


@app.route("/image_stats/user/<path:user>/monthly", methods=["GET"])
def image_user_monthly(user):
    current_year = date.today().year
    mode = normalize_image_stats_mode(request.args.get("mode", "images"))
    fetch_rows = (
        fetch_user_parts_imaged_monthly_totals
        if mode == "parts_imaged"
        else fetch_user_images_monthly_totals
    )

    rows = fetch_rows(user, current_year)

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


@app.route("/image_stats/user/<path:user>/daily", methods=["GET"])
def image_user_daily(user):
    try:
        month = int(request.args.get("month", "1"))
    except ValueError:
        month = 1

    current_year = date.today().year
    mode = normalize_image_stats_mode(request.args.get("mode", "images"))
    fetch_rows = (
        fetch_user_parts_imaged_daily_totals
        if mode == "parts_imaged"
        else fetch_user_images_daily_totals
    )

    rows = fetch_rows(user, current_year, month)

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

    resolved_dimension = "user" if normalize_stats_dimension(dimension) == "user" else "department"
    user = session.get("username")
    persist_stats_exclusions(user, resolved_dimension, excluded_departments)

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


@app.route("/vehicle_stats/exclusions", methods=["POST"])
def save_vehicle_stats_exclusions():
    filter_type = request.form.get("filter", "today")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    excluded_companies = request.form.getlist("exclude")
    group_mode = request.form.get("group", "company")

    user = session.get("username")
    persist_stats_exclusions(user, "insurance_company", excluded_companies)

    return redirect(
        url_for(
            "vehicle_stats",
            filter=filter_type,
            start_date=start_date,
            end_date=end_date,
            exclude=excluded_companies,
            group=group_mode,
        )
    )


@app.route("/image_timeline/exclusions", methods=["POST"])
def save_image_timeline_exclusions():
    filter_type = request.form.get("filter", "this_month")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    excluded_users = request.form.getlist("exclude")

    user = session.get("username")
    persist_stats_exclusions(user, "user", excluded_users)

    return redirect(
        url_for(
            "image_timeline",
            filter=filter_type,
            start_date=start_date,
            end_date=end_date,
            exclude=excluded_users,
        )
    )


@app.route("/stores_timeline/exclusions", methods=["POST"])
def save_stores_timeline_exclusions():
    filter_type = request.form.get("filter", "this_month")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    excluded_users = request.form.getlist("exclude")

    user = session.get("username")
    persist_stats_exclusions(user, "user", excluded_users)

    return redirect(
        url_for(
            "stores_timeline",
            filter=filter_type,
            start_date=start_date,
            end_date=end_date,
            exclude=excluded_users,
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
