# import requests, os, csv, re, html, json
# from bs4 import BeautifulSoup

# url = "https://www.loosegrowndiamond.com/wp-admin/admin-ajax.php"

# headers = {
#     "accept": "application/json, text/javascript, */*; q=0.01",
#     "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
#     "origin": "https://www.loosegrowndiamond.com",
#     "referer": "https://www.loosegrowndiamond.com/inventory/",
#     "user-agent": "Mozilla/5.0",
#     "x-requested-with": "XMLHttpRequest",
# }

# PAGE_SIZE = 1000
# STATE_FILE = "lgd_state.txt"
# CSV_FILE = "loosegrowndiamond.csv"

# # ----------------------------
# # Resume
# # ----------------------------
# start = 410

# def clean_id(v):
#     if not v:
#         return ""
#     return v.replace('"', "").strip()

# existing_ids = set()

# if os.path.exists(CSV_FILE):
#     with open(CSV_FILE, newline="", encoding="utf-8") as f:
#         reader = csv.DictReader(f)
#         for row in reader:
#             iid = clean_id(row.get("sku"))
#             if iid:
#                 existing_ids.add(iid)

# print("Already have", len(existing_ids), "records")
# print("Starting from:", start)

# # ----------------------------
# # Payload
# # ----------------------------
# def get_payload(start):
#     return {
#         "action": "ls_loadmore_inventory",
#         "start": str(start),
#         "ls_per_page": str(PAGE_SIZE),
#         "price_range": "100.00,100000.00",
#         "shape[]": ["Round","Princess","Cushion","Oval","Emerald","Pear","Asscher","Radiant","Marquise","Heart"],
#         "carat_range": "0.00,62.96",
#         "cut_range": "0.00,4.00",
#         "color_range": "1,10.00",
#         "clarity_range": "1,11.00",
#         "depth_range": "46.00,78.00",
#         "table_range": "50.00,80.00",
#         "lwratio": "1.00,2.75",
#         "heartarrow": "0"
#     }

# def clean_html(v):
#     if not v:
#         return ""
#     v = html.unescape(v)
#     v = re.sub(r"<[^>]+>", "", v)
#     v = v.replace("\\n", " ").replace("\\t", " ").replace("\\", "")
#     return re.sub(r"\s+", " ", v).strip()

# if not os.path.exists(CSV_FILE):
#     with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
#         csv.writer(f).writerow([
#             "shape","carat","cut","color","clarity","price","data_iid","data_id","sku"
#         ])

# while True:
#     print("Fetching start =", start)

#     r = requests.post(url, headers=headers, data=get_payload(start), timeout=60)

#     data = r.json()
#     total=data.get('total')
#     print('total',total,next)
#     # print(next)
#     next=data.get('next','')
#     html_block = data.get("content", "")

#     soup = BeautifulSoup(html_block, "lxml")
#     trs = soup.select("tr[data-iid]")

#     if not trs:
#         print("No more data")
#         break

#     with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
#         writer = csv.writer(f)

#         for tr in trs:
#             tds = tr.find_all("td", recursive=False)
#             if len(tds) < 6:
#                 continue

#             iid = clean_id(tr.get("data-iid"))
#             did = clean_id(tr.get("data-id"))

#             sku = ""
#             for c in tr.get("class", []):
#                 if c.startswith("cls"):
#                     sku = c.replace("cls", "")
#                     break

#             if not sku:
#                 continue

#             if iid in existing_ids:
#                 continue

#             sale = tds[5].select_one(".ls_sprice")
#             if sale:
#                 price = clean_html(sale.decode_contents()).replace("$","")
#             else:
#                 raw = clean_html(tds[5].decode_contents())
#                 nums = re.findall(r"\d+", raw)
#                 price = min(map(int, nums)) if nums else ""

#             writer.writerow([
#                 clean_html(tds[0].decode_contents()).split()[0].lower(),
#                 clean_html(tds[1].decode_contents()),
#                 clean_html(tds[2].decode_contents()),
#                 clean_html(tds[3].decode_contents()),
#                 clean_html(tds[4].decode_contents()),
#                 price,
#                 iid,
#                 did,
#                 sku
#             ])

#             existing_ids.add(sku)

#     start =next
#     open(STATE_FILE, "w").write(str(start))
#     print("Saved up to", start)

# print("ALL DATA DOWNLOADED CLEANLY")

import requests, os, csv, re, html
from bs4 import BeautifulSoup

URL = "https://www.loosegrowndiamond.com/wp-admin/admin-ajax.php"

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://www.loosegrowndiamond.com",
    "referer": "https://www.loosegrowndiamond.com/inventory/",
    "user-agent": "Mozilla/5.0",
    "x-requested-with": "XMLHttpRequest",
}

PAGE_SIZE = 1000
CSV_FILE = "loosegrowndiamond.csv"
STATE_FILE = "lgd_state.txt"



def clean_id(v):
    return v.replace('"', "").strip() if v else ""


def clean_html(v):
    if not v:
        return ""
    v = html.unescape(v)
    v = re.sub(r"<[^>]+>", "", v)
    v = v.replace("\\n", " ").replace("\\t", " ").replace("\\", "")
    return re.sub(r"\s+", " ", v).strip()


def get_payload(start):
    return {
        "action": "ls_loadmore_inventory",
        "start": str(start),
        "ls_per_page": str(PAGE_SIZE),
        "price_range": "100.00,100000.00",
        "shape[]": ["Round","Princess","Cushion","Oval","Emerald","Pear","Asscher","Radiant","Marquise","Heart"],
        "carat_range": "0.00,62.96",
        "cut_range": "0.00,4.00",
        "color_range": "1,10.00",
        "clarity_range": "1,11.00",
        "depth_range": "46.00,78.00",
        "table_range": "50.00,80.00",
        "lwratio": "1.00,2.75",
        "heartarrow": "0"
    }


def load_existing_skus():
    skus = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sku = clean_id(row.get("sku"))
                if sku:
                    skus.add(sku)
    print("Already have", len(skus), "records")
    return skus

def get_start_value():
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w") as f:
            f.write("1")
        return 1

    try:
        return int(open(STATE_FILE).read().strip())
    except:
        with open(STATE_FILE, "w") as f:
            f.write("1")
        return 1

def save_state(start):
    with open(STATE_FILE, "w") as f:
        f.write(str(start))


def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "shape","carat","cut","color","clarity","price","data_iid","data_id","sku"
            ])



def extract_price(td):
    sale = td.select_one(".ls_sprice")
    if sale:
        return clean_html(sale.decode_contents()).replace("$","")

    raw = clean_html(td.decode_contents())
    nums = re.findall(r"\d+", raw)
    return min(map(int, nums)) if nums else ""


def fetch_page(start):
    r = requests.post(URL, headers=HEADERS, data=get_payload(start), timeout=60)
    data = r.json()
    return data.get("content", ""), data.get("next", "")


def parse_rows(html_block, existing_ids, writer):
    soup = BeautifulSoup(html_block, "lxml")
    trs = soup.select("tr[data-iid]")

    for tr in trs:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 6:
            continue

        iid = clean_id(tr.get("data-iid"))
        did = clean_id(tr.get("data-id"))

        sku = ""
        for c in tr.get("class", []):
            if c.startswith("cls"):
                sku = c.replace("cls", "")
                break

        if not sku or sku in existing_ids:
            continue

        price = extract_price(tds[5])

        writer.writerow([
            clean_html(tds[0].decode_contents()).split()[0].lower(),
            clean_html(tds[1].decode_contents()),
            clean_html(tds[2].decode_contents()),
            clean_html(tds[3].decode_contents()),
            clean_html(tds[4].decode_contents()),
            price,
            iid,
            did,
            sku
        ])

        existing_ids.add(sku)

    return len(trs)

def run_scraper():
    init_csv()
    existing_ids = load_existing_skus()
    start = get_start_value()

    print("Starting from:", start)

    while True:
        print("Fetching start =", start)

        html_block, next_start = fetch_page(start)

        if not html_block:
            print("No more data")
            break

        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            rows = parse_rows(html_block, existing_ids, writer)

        if rows == 0:
            break

        start = next_start
        save_state(start)
        print("Saved up to", start)

    print("ALL DATA DOWNLOADED CLEANLY")



if __name__ == "__main__":
    run_scraper()
