# %%
from typing import TypedDict, Annotated, Literal
#from langgraph.graph import StateGraph, START, END
import fitz  # PyMuPDF
import requests
import random
import io
from PIL import Image
from langdetect import detect, LangDetectException

# %%
import pytesseract

# This links your Python library to the software you just installed
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# %%
fraction_doc=0.3 #limit to what fraction of doc should be covered with text to not pass for OCR

# %%
def getdoc(source:str):
    if isinstance(source,str) and source.startswith("http"):
        resp=requests.get(source, timeout=30)
        resp.raise_for_status()
        doc=fitz.open(stream=resp.content,filetype="pdf")
    else:
        doc = fitz.open(str(source))
    return doc

# %%
def get_text_fraction(doc, pages_to_include):
    """
    Calculates text area fraction for a list of pages in an open fitz.Document.
    :param doc: An opened fitz.Document object
    :param pages_to_include: List of zero-indexed page numbers
    :return: Fraction (float) of text area vs total page area
    """
    total_text_area = 0
    total_page_area = 0
    
    for p_no in pages_to_include:
        # Safety check: skip if page index is out of bounds
        if p_no < 0 or p_no >= doc.page_count:
            continue
            
        page = doc[p_no]
        
        # 1. Page Area (Standard PDF points)
        total_page_area += page.rect.width * page.rect.height
        
        # 2. Text Block Area 
        # get_text("blocks") returns a list of tuples: (x0, y0, x1, y1, "text", block_no, block_type)
        for b in page.get_text("blocks"):
            x0, y0, x1, y1 = b[:4]
            block_area = (x1 - x0) * (y1 - y0)
            total_text_area += block_area
            
    if total_page_area == 0:
        return 0.0
        
    return total_text_area / total_page_area

# %%
def is_OCR_required(source):
    num_pages = source.page_count
    sample_pages=random.sample(range(1,num_pages+1),max(3,int(num_pages*0.1)))
    fraction=get_text_fraction(source,sample_pages)
    if fraction<fraction_doc:
        return True
    else:
        return False

# %%
def OCR(doc: fitz.Document) -> str:
    """Extract text from all images embedded in a PDF via OCR."""
    md_sections: list[str] = []

    for page_index in range(len(doc)):
        page = doc[page_index]
        image_list = page.get_images(full=True)

        if not image_list:
            continue  # nothing to OCR on this page

        page_texts: list[str] = []

        for img_index, img_info in enumerate(image_list):
            xref = img_info[0]  # cross-reference number of the image

            # Extract raw image bytes from the document
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]

            # Convert to a PIL Image and run OCR
            pil_image = Image.open(io.BytesIO(image_bytes))
            ocr_text = pytesseract.image_to_string(pil_image).strip()

            if ocr_text:
                page_texts.append(
                    f"**Image {img_index + 1}**\n\n{ocr_text}"
                )

        if page_texts:
            section = (
                f"## Page {page_index + 1}\n\n"
                + "\n\n---\n\n".join(page_texts)
            )
            md_sections.append(section)

    if not md_sections:
        return "*No images with extractable text were found in this document.*"

    return "\n\n".join(md_sections)

# %%
def extract(doc: fitz.Document) -> str:
    """
    Extract native text layers from every page of a PDF.
    (Renamed from extract_text to match the custom pipeline snippet).
    """
    md_sections: list[str] = []

    for page_index in range(len(doc)):
        page = doc[page_index]
        raw_text = page.get_text("text").strip()

        if not raw_text:
            continue  # blank or fully image-based page

        section = f"## Page {page_index + 1}\n\n{raw_text}"
        md_sections.append(section)

    if not md_sections:
        return "*No native text layers were found in this document.*"

    return "\n\n".join(md_sections)

# %%
def _is_english(text: str) -> bool:
    """Return True if *text* is detected as English, False otherwise."""
    if len(text.strip()) < 20:
        return False
    try:
        return detect(text) == "en"
    except LangDetectException:
        return False


def english_filtered_text(text: str) -> str:
    """Return only English content from a Markdown-formatted PDF string."""
    import re

    page_pattern = re.compile(r"(?=^## Page \d+)", re.MULTILINE)
    page_sections = [s.strip() for s in page_pattern.split(text) if s.strip()]

    output_sections: list[str] = []

    for section in page_sections:
        lines = section.split("\n", 1)
        if len(lines) == 2 and lines[0].startswith("## Page"):
            page_heading = lines[0].strip()
            body = lines[1].strip()
        else:
            page_heading = None
            body = section

        raw_blocks = [b.strip() for b in body.split("\n\n---\n\n") if b.strip()]
        english_blocks: list[str] = []

        for block in raw_blocks:
            block_lines = block.split("\n\n", 1)
            if (
                len(block_lines) == 2
                and block_lines[0].startswith("**")
                and block_lines[0].endswith("**")
            ):
                label   = block_lines[0]
                content = block_lines[1]
            else:
                label   = None
                content = block

            if _is_english(content):
                english_blocks.append(block)

        if not english_blocks:
            continue

        body_md = "\n\n---\n\n".join(english_blocks)
        if page_heading:
            output_sections.append(f"{page_heading}\n\n{body_md}")
        else:
            output_sections.append(body_md)

    return "\n\n".join(output_sections)

# %%
def extract_text(path: str) -> str:
    """Main pipeline combining reading, native vs OCR parsing, and language filtering."""
    doc = getdoc(path)
    method = is_OCR_required(doc)
    if method:
        text = OCR(doc)
    else:
        text = extract(doc)
    text = english_filtered_text(text)
    return text


# %%
####  text=extract_text("C:\\Users\\vaibh\\Downloads\\L.A.BILL 18 of 2021.pdf")