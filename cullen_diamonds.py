from curl_cffi import requests
import csv, os, time, json
from fake_useragent import UserAgent
import cv2, numpy as np, pandas as pd, psutil
import requests, time
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter


CSV_FILE = "cullen_diamonds.csv"
URL = "https://api.cullenjewellery.com/api/stone_preference_data/feed"

ua = UserAgent()

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://cullenjewellery.com",
    "referer": "https://cullenjewellery.com/",
    "user-agent": ua.random,
}

BASE_FILTER = {"filter":
    {"type":"diamond",
    "shapes": ["round","oval","emerald","radiant","pear","cushion","elongated_cushion","elongated_hexagon","marquise","princess","asscher","heart"],
    "minCarat":2,
    "maxCarat":10,
    "minClarity":"i1",
    "maxClarity":"if1",
    "minColour":"j",
    "maxColour":"d",
    "minCut":"ex",
    "maxCut":"id",
    "moissaniteColours":["colourless","clover_green","smokey_grey","sunshine_yellow","champagne","midnight_blue","seafoam_green","sandy_yellow"],
    "sapphireColours":["kashmir_blue","sky_blue","royal_purple","scarlet_red","flamingo_pink","blossom_pink"],
    "fancyColours":["fancy_pink"],
    "minFancyColour":"faint",
    "maxFancyColour":"fancy_deep",
    "minDwRatio":0.5,
    "maxDwRatio":0.9,"minLwRatio":0.5,
    "maxLwRatio":2.5,"minTwRatio":0.5,"maxTwRatio":0.9},"sort":"recommended","addedRanOut":False,"currency":"USD"}
FIELDS = [
    "id","price","dimensions","carat","length","width","depth","table",
    "dw_ratio","lw_ratio","tw_ratio","is_wide","shape","cut","colour",
    "colour_rank","colour_intensity","clarity","clarity_rank","polish",
    "symm","culet","lab","certificate_number"
]


def load_existing_ids():
    ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ids.add(row["id"])
    return ids

def save_diamonds(diamonds, writer, existing_ids):
    new = 0
    for d in diamonds:
        data = d.get("info")
        if not data:
            continue

        did = data.get("id")
        if not did or did in existing_ids:
            continue

        writer.writerow({
            "id": did,
            "price": data.get("price"),
            "dimensions": data.get("dimensions"),
            "carat": data.get("carat"),
            "length": data.get("length"),
            "width": data.get("width"),
            "depth": data.get("depth"),
            "table": data.get("table"),
            "dw_ratio": data.get("dw_ratio"),
            "lw_ratio": data.get("lw_ratio"),
            "tw_ratio": data.get("tw_ratio"),
            "is_wide": data.get("is_wide"),
            "shape": data.get("shape"),
            "cut": data.get("cut"),
            "colour": data.get("colour"),
            "colour_rank": data.get("colour_rank"),
            "colour_intensity": data.get("colour_intensity"),
            "clarity": data.get("clarity"),
            "clarity_rank": data.get("clarity_rank"),
            "polish": data.get("polish"),
            "symm": data.get("symm"),
            "culet": data.get("culet"),
            "lab": data.get("lab"),
            "certificate_number": None
        })

        existing_ids.add(did)
        new += 1

    return new


def fetch_all_diamonds():
    session = requests.Session()
    existing_ids = load_existing_ids()
    file_exists = os.path.exists(CSV_FILE)

    with open(CSV_FILE, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()

        start = 0
        page = 1

        while True:
            payload = BASE_FILTER.copy()
            payload["start"] = start

            r = session.post(URL, headers=HEADERS, data=json.dumps(payload))
            if r.status_code != 200:
                print("Blocked, retrying...")
                time.sleep(15)
                continue

            stones = r.json().get("stones", [])
            if not stones:
                print("Finished downloading.")
                break

            inserted = save_diamonds(stones, writer, existing_ids)

            print(f"Page {page} | Inserted {inserted}")

            page += 1
            start += 20
            time.sleep(1)
            
# #######################################################################qr code scan and convert findout certficate_number ###############

INPUT_CSV  = "cullen_diamonds.csv"
OUTPUT_CSV = "cullen_cert2_diamonds.csv"

MAX_WORKERS = 10
CPU_LIMIT = 80

detector = cv2.QRCodeDetector()
session = requests.Session()
session.mount("https://", HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS))


def wait_for_cpu():
    while psutil.cpu_percent(interval=1) > CPU_LIMIT:
        time.sleep(2)


def get_certificate(diamond_id):
    try:
        url = f"https://diamonds.cullenjewellery.com/{diamond_id}/cert.jpeg"
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            return diamond_id, None

        img = cv2.imdecode(np.frombuffer(r.content, np.uint8), cv2.IMREAD_GRAYSCALE)
        text, _, _ = detector.detectAndDecode(img)

        return diamond_id, text.split("=")[-1] if text else None
    except:
        return diamond_id, None


def process_certificates():
    df = pd.read_csv(INPUT_CSV)
    ids = df["id"].astype(str).tolist()

    df.iloc[0:0].assign(certificate_number="").to_csv(OUTPUT_CSV, index=False)

    batch = 500

    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        for i in range(0, len(ids), batch):
            wait_for_cpu()

            chunk = ids[i:i+batch]
            results = dict(pool.map(get_certificate, chunk))

            out = df[df["id"].astype(str).isin(chunk)].copy()
            out["certificate_number"] = out["id"].astype(str).map(results)
            out.to_csv(OUTPUT_CSV, mode="a", header=False, index=False)

            print(f"Batch {i//batch+1} completed")
            
if __name__ == "__main__":
    # fetch_all_diamonds()      
    process_certificates()   
