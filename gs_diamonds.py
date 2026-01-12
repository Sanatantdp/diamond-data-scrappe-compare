# from curl_cffi import requests
# import json
# import os
# from bs4 import BeautifulSoup
# import csv


# CSV_FILE="gs_diamonds_data.csv"

# existing_ids = set()
# if os.path.exists(CSV_FILE):
#     with open(CSV_FILE, newline="", encoding="utf-8") as f:
#         reader = csv.DictReader(f)
#         for row in reader:
#             existing_ids.add(row["cert_no"])

# def parse_gs_cards_to_csv(cards, csv_file="gs_diamonds.csv"):

#     fieldnames = [
#         "cert_no",
#         "carat",
#         "color",
#         "clarity",
#         "price_aud",
#         "url"
#     ]

#     with open(csv_file, "a", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()

#         for card in cards:
#             data = {}  

#             a = card.select_one("a.font-semibold")
#             if not a:
#                 continue

#             data["url"] = a["href"]

           
#             data["cert_no"] = data["url"].split("gs")[-1].split("_")[0]
            
#             if data['cert_no'] in existing_ids:
#                 continue
            
#             existing_ids.add(data['cert_no'])
           
          
#             spans = a.find_all("span")
#             if len(spans) >= 2:
#                 data["carat"] = spans[0].get_text(strip=True).replace("ct", "")
#                 cc = spans[1].get_text(strip=True)
#                 if "/" in cc:
#                     data["color"], data["clarity"] = cc.split("/", 1)

         
#             price = card.select_one(".text-primary-dark")
#             if price:
#                 data["price_aud"] = price.get_text(strip=True).replace("A$", "").replace(",", "")

#             # btn = card.select_one("[data-compare-variant_id-param]")
#             # if btn:
#             #     data["variant_id"] = btn["data-compare-variant_id-param"]

#             writer.writerow(data)

#     print("Saved:", csv_file)


# base_dir = os.getcwd()
# gs_dir = os.path.join(base_dir, "gs_pages")

# all_html_blocks = []

# for filename in os.listdir(gs_dir):

#     if not filename.endswith(".json"):
#         continue

#     file_path = os.path.join(gs_dir, filename)
#     print(file_path)
    
#     with open(file_path, "r", encoding="utf-8") as f:
#         data = json.load(f)
#     commands = data.get("commands", [])

#     if not commands:
#         print("No commands in", filename)
#         continue

#     html = commands[0].get("html")
#     if not html:
#         print("No html in", filename)
#         continue

#     soup = BeautifulSoup(html, "lxml")
#     grid = soup.select_one("div.product-list")

#     if not grid:
#         print("Product grid not found")
#         cards = []
#     else:
#         # All product cards
#         cards = grid.select("div.bg-white")
#         parse_gs_cards_to_csv(cards)
      
#     print("Total products found:", len(cards))



from curl_cffi import requests
import json, os, csv
from bs4 import BeautifulSoup

CSV_FILE = "gs_diamonds_data.csv"
GS_DIR = "gs_pages"

def load_existing_ids(csv_file):
    ids = set()
    if os.path.exists(csv_file):
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ids.add(row["cert_no"])
    return ids

def ensure_csv(csv_file):
    if not os.path.exists(csv_file):
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "cert_no", "carat", "color", "clarity", "price_aud", "url"
            ])
            writer.writeheader()


def parse_gs_cards_to_csv(cards, csv_file, existing_ids):

    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "cert_no", "carat", "color", "clarity", "price_aud", "url"
        ])

        for card in cards:
            a = card.select_one("a.font-semibold")
            if not a:
                continue

            url = a["href"]
            cert_no = url.split("gs")[-1].split("_")[0]

            if cert_no in existing_ids:
                continue

            existing_ids.add(cert_no)

            data = {
                "url": url,
                "cert_no": cert_no,
                "carat": "",
                "color": "",
                "clarity": "",
                "price_aud": ""
            }

            spans = a.find_all("span")
            if len(spans) >= 2:
                data["carat"] = spans[0].get_text(strip=True).replace("ct", "")
                cc = spans[1].get_text(strip=True)
                if "/" in cc:
                    data["color"], data["clarity"] = cc.split("/", 1)

            price = card.select_one(".text-primary-dark")
            if price:
                data["price_aud"] = (
                    price.get_text(strip=True)
                    .replace("A$", "")
                    .replace(",", "")
                )

            writer.writerow(data)


def parse_gs_json_file(file_path, csv_file, existing_ids):

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    commands = data.get("commands", [])
    if not commands:
        print("No commands in", file_path)
        return 0

    html = commands[0].get("html")
    if not html:
        print("No html in", file_path)
        return 0

    soup = BeautifulSoup(html, "lxml")
    grid = soup.select_one("div.product-list")

    if not grid:
        print("Product grid not found in", file_path)
        return 0

    cards = grid.select("div.bg-white")
    parse_gs_cards_to_csv(cards, csv_file, existing_ids)

    return len(cards)


def run_gs_scraper(gs_dir=GS_DIR, csv_file=CSV_FILE):

    ensure_csv(csv_file)
    existing_ids = load_existing_ids(csv_file)

    total = 0

    for filename in os.listdir(gs_dir):
        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(gs_dir, filename)
        print("Processing:", file_path)

        count = parse_gs_json_file(file_path, csv_file, existing_ids)
        print("Products found:", count)

        total += count

    print("Total products processed:", total)


if __name__ == "__main__":
    run_gs_scraper()
