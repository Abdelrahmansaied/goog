import pandas as pd
import openpyxl
import re
import time
import random
import threading
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from difflib import get_close_matches
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
import io
import streamlit as st

# Setup Chrome options
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
chrome_options.add_argument(f"user-agent={user_agent}")

results_lock = threading.Lock()  # Thread lock to manage concurrent writes

def duckduckgo_search(query, result_dict, index, domain):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    url = f'https://duckduckgo.com/?q={query}'
    driver.get(url)
    time.sleep(random.uniform(2, 4))  # Wait for the page to load

    links = []

    try:
        # Extract initial links
        links += extract_links(driver)

        # Click the 'More Results' button until we have 30 links
        while len(links) < 30:
            try:
                more_results_button = driver.find_element(By.XPATH, '//*[@id="more-results"]')
                more_results_button.click()
                time.sleep(random.uniform(2, 4))  # Wait for new results to load
                links += extract_links(driver)

                # Remove duplicates
                links = list(set(links))

            except Exception as e:
                break

    except Exception as e:
        print(f"Error extracting links: {e}")
    
    driver.quit()  # Close the WebDriver

    # Use lock to safely update the result
    with results_lock:
        result_dict[index] = (links, filter_and_search_content(links, query, domain))

def extract_links(driver):
    links = []
    for i in range(0, 30):  # Check for up to 30 elements
        element_id = f"r1-{i}"
        try:
            elements = driver.find_elements(By.XPATH, f'//*[@id="{element_id}"]/div[2]/h2/a')
            for element in elements:
                link = element.get_attribute('href')
                if link:
                    links.append(link)
        except Exception as e:
            continue
    return links

def filter_and_search_content(links, mpn, domain):
    mpn_pattern = re.compile(re.escape(mpn), re.IGNORECASE)
    best_match = None

    # Filter links for the specified domain
    filtered_links = [link for link in links if domain in link]

    if not filtered_links:
        return ["No links found for this domain."]

    for link in filtered_links:
        try:
            response = requests.get(link)
            content_type = response.headers.get('Content-Type', '')

            if 'text/html' in content_type:
                soup = BeautifulSoup(response.content, 'html.parser')
                text = soup.get_text()
                if mpn_pattern.search(text):
                    return [link]  # Return immediately if exact match is found

            elif 'application/pdf' in content_type:
                with io.BytesIO(response.content) as f:
                    reader = PdfReader(f)
                    pdf_text = ''
                    for page in reader.pages:
                        pdf_text += page.extract_text() or ''
                    if mpn_pattern.search(pdf_text):
                        return [link]  # Return if exact match is found

            # If no exact match, track the best match
            if best_match is None:
                best_match = link
            else:
                close_matches = get_close_matches(mpn, [link], n=1, cutoff=0.6)
                if close_matches:
                    best_match = close_matches[0]

        except Exception as e:
            continue
    
    return [best_match] if best_match else ["No suitable match found."]

def extract_domain_prefix(manufacturer_name):
    return manufacturer_name.lower() + ".com"  # Adjust as needed

# Streamlit UI
st.title("MPN Search Application")
st.write("Upload an Excel file with columns 'MPN' and 'SE_MAN_NAME'.")

uploaded_file = st.file_uploader("Choose an Excel file", type='xlsx')

if uploaded_file and st.button("Start Search"):
    df = pd.read_excel(uploaded_file)

    if 'MPN' not in df.columns or 'SE_MAN_NAME' not in df.columns:
        st.error("Input file must contain 'MPN' and 'SE_MAN_NAME' columns.")
    else:
        df['Online Link'] = ''
        result_dict = {}

        threads = []
        for index, row in df.iterrows():
            mpn = row['MPN']
            se_man_name = row['SE_MAN_NAME']
            
            search_domain = extract_domain_prefix(se_man_name)
            search_query = f"{mpn}"
            thread = threading.Thread(target=duckduckgo_search, args=(search_query, result_dict, index, search_domain))
            threads.append(thread)
            thread.start()
            time.sleep(random.uniform(3, 10))  # Random sleep time

        for thread in threads:
            thread.join()

        for index, row in df.iterrows():
            se_man_name = row['SE_MAN_NAME']
            links, results = result_dict.get(index, ([], []))
            if links:
                st.write(f"Links found for {mpn}:")
                for link in links:
                    st.write(link)
            else:
                st.write(f"No links found for {mpn}.")

            if results:
                for result in results:
                    if isinstance(result, str):
                        st.write(f"Result for {mpn}: {result}")

            # Update the Online Link column
            for link in links:
                domain_prefix = extract_domain_prefix(se_man_name).lower()
                if domain_prefix in link:
                    df.at[index, 'Online Link'] = link
                    break

        output_file = r'C:\report\output_file.xlsx'
        df.to_excel(output_file, index=False)

        st.success("Process completed! Results saved to `output_file.xlsx`.")
        st.dataframe(df)
