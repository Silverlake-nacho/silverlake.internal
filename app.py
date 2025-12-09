from flask import Flask, request, render_template, send_file, redirect, url_for, session, flash
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
import psycopg2
from datetime import date, timedelta
import time
import requests
from bs4 import BeautifulSoup
from flask import request, render_template_string
from collections import defaultdict
from sshtunnel import SSHTunnelForwarder


def rgb_to_hex(rgb):
    r = int(rgb.get('red', 1) * 255)
    g = int(rgb.get('green', 1) * 255)
    b = int(rgb.get('blue', 1) * 255)
    return '#{:02X}{:02X}{:02X}'.format(r, g, b)

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

USERS = {
    'admin': 'Silverlake1!',
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
    
from datetime import date, timedelta

@app.route("/logs", methods=["GET", "POST"])
def logs():
    filter_type = request.args.get("filter", "today")
    start_date = None
    end_date = None

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
