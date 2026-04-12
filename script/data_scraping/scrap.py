import time
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin
import markdownify

import os
from dotenv import load_dotenv
load_dotenv()

# Import your custom PDF extraction pipeline
from pdf_extractor import extract_text
# ==========================================
# CONFIGURATION
# ==========================================
# Replace with your actual database password
MONGO_URI = os.getenv("MONGO_URL")
DATABASE_NAME = "policy_AI"         
COLLECTION_NAME = "Bills parliament" 
BASE_URL = "https://prsindia.org"
START_URL = f"{BASE_URL}/billtrack"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# ==========================================
# SCRAPING UTILITIES
# ==========================================

def scrape_bill_details(bill_url):
    """Extracts summary and uses custom PDF pipeline from the bill's page."""
    data = {"summary": "N/A", "pdf_content": "N/A"}
    try:
        res = requests.get(bill_url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')

        # 1. Extract Summary and convert to Markdown
        summary_div = soup.find('div', class_='field-item even', attrs={"property": "content:encoded"})
        if summary_div:
            data["summary"] = markdownify.markdownify(str(summary_div), heading_style="ATX").strip()

        # 2. Extract PDF Link and Use Custom Pipeline
        pdf_link_tag = soup.find('a', class_='pdf-link')
        if pdf_link_tag and pdf_link_tag.get('href'):
            pdf_url = urljoin(BASE_URL, pdf_link_tag.get('href'))
            print(f"    - Processing PDF via custom pipeline: {pdf_url}")
            
            # Pass the URL directly to your custom pipeline
            try:
                extracted_content = extract_text(pdf_url)
                if extracted_content and extracted_content.strip():
                    data["pdf_content"] = f"## PDF Content\n\n{extracted_content}"
                else:
                    data["pdf_content"] = "_[No English text could be extracted from PDF]_"
            except Exception as e:
                 print(f"    ! Error in PDF pipeline for {pdf_url}: {e}")
                 data["pdf_content"] = f"_[Pipeline extraction failed: {str(e)}]_"

    except Exception as e:
        print(f"    ! Error loading page {bill_url}: {e}")
        
    return data

# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    print("Connecting to MongoDB Atlas...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    print(f"Scraping list from: {START_URL}")
    res = requests.get(START_URL, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    # Selecting the rows
    bill_rows = soup.find_all('div', class_='views-row')
    
    for row in bill_rows:
        title_link = row.find('h3', class_='cate').find('a') if row.find('h3', class_='cate') else None
        if not title_link: continue

        heading = title_link.get_text(strip=True)
        link = urljoin(BASE_URL, title_link.get('href'))
        
        status_div = row.find('div', class_=lambda x: x and 'status' in x.lower())
        status = status_div.get_text(strip=True) if status_div else "Unknown"

        print(f"\n[*] Heading: {heading}")
        print(f"    Status: {status}")

        # Deep scrape for summary and PDF
        details = scrape_bill_details(link)

        # Prepare payload
        payload = {
            "heading": heading,
            "link": link,
            "status": status,
            "summary_markdown": details["summary"],
            "pdf_content_markdown": details["pdf_content"],
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # Upsert into Atlas
        collection.update_one({"link": link}, {"$set": payload}, upsert=True)
        print("    -> Uploaded to 'all-policies' cluster.")
        
        # Pause to avoid rate limits
        time.sleep(2)

if __name__ == "__main__":
    main()