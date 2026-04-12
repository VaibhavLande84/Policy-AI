import os
from dotenv import load_dotenv
load_dotenv()
import requests
import time
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin
import markdownify
# Import your custom PDF extraction pipeline
from pdf_extractor import extract_text 
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pymongo import MongoClient
import time
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from scrap import extract_text
# --- GROQ CONFIGURATION ---
# Ensure GROQ_API_KEY is set in your environment variables
llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    temperature=0,
    api_key=os.getenv("GROQ_API_KEY")
)

summary_prompt = ChatPromptTemplate.from_messages([
    ("system", "You are an expert legal analyst. Summarize the following parliamentary bill text into a concise Markdown summary. Focus on the purpose, key provisions, and impact."),
    ("user", "Text: {text}")
])

def generate_summary(text):
    """Uses Groq via LangChain to summarize the PDF content."""
    if not text or len(text) < 100:
        return "Content too short for summary."
    
    try:
        # Truncate text if it's extremely long to stay within context limits
        truncated_text = text[:12000] 
        chain = summary_prompt | llm
        response = chain.invoke({"text": truncated_text})
        return response.content
    except Exception as e:
        print(f"    ! Groq Summary Error: {e}")
        return "Error generating summary."

def scrape_bill_details(pdf_url):
    """Extracts PDF content and generates an AI summary."""
    data = {"summary": "N/A", "pdf_content": "N/A"}
    
    try:
        print(f"    - Extracting PDF: {pdf_url}")
        extracted_content = extract_text(pdf_url) # Your existing function
        
        if extracted_content and extracted_content.strip():
            data["pdf_content"] = extracted_content
            # --- NEW SUMMARY STEP ---
            print("    - Generating AI Summary via Groq...")
            data["summary"] = generate_summary(extracted_content)
        else:
            data["pdf_content"] = "_[No text extracted]_"
            
    except Exception as e:
        print(f"    ! Error processing {pdf_url}: {e}")
        
    return data

def main():
    MONGO_URI = os.getenv("MONGO_URL")
    DATABASE_NAME = "policy_AI"         
    COLLECTION_NAME = "Acts Parliament" 
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    BASE_URL = "https://prsindia.org"
    START_URL = f"{BASE_URL}/acts/parliament"
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

    res = requests.get(START_URL, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    bill_rows = soup.find_all('div', class_='views-row')
    
    for row in bill_rows:
        container = row.find('div', class_='col-11 pl-0')
        link_tag = container.find('a') if container else None
        if not link_tag: continue

        heading = link_tag.get_text(strip=True)
        pdf_link = urljoin(BASE_URL, link_tag.get('href'))
        
        print(f"\n[*] Processing: {heading}")

        # Get PDF content and AI Summary
        details = scrape_bill_details(pdf_link)

        payload = {
            "heading": heading,
            "link": pdf_link,
            "ai_summary": details["summary"],
            "full_content": details["pdf_content"],
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        collection.update_one({"link": pdf_link}, {"$set": payload}, upsert=True)
        print("    -> Data & Summary uploaded to MongoDB.")
        time.sleep(2)

if __name__ == "__main__":
    main()