import warnings

warnings.filterwarnings("ignore")

import pandas as pd
import os
import glob
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import os
from aiofiles import open as aio_open
from aiohttp import ClientSession
import re
import aiofiles

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("scrape.log"), logging.StreamHandler()],
)


headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
}

logging.getLogger("aiohttp.client").setLevel(logging.ERROR)


async def fetch(session, url, retries=5, backoff_factor=0.5):
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers) as response:
                return await response.text()
        except aiohttp.ClientOSError as e:
            logging.warning(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < retries - 1:
                sleep_time = backoff_factor * (2**attempt)
                logging.info(f"Retrying in {sleep_time} seconds...")
                await asyncio.sleep(sleep_time)
            else:
                logging.error(f"All retry attempts failed for URL: {url}")
                raise


# The get_images function doesn't need to be asynchronous since it's just parsing the soup object
def get_images(soup):
    image_urls = []
    scripts = soup.find_all("script")
    for script in scripts:
        if "window.PRELOADED_STATE" in script.text:
            matches = re.findall(
                r'https://onemg.gumlet.io/l_watermark_346,w_480,h_480/a_ignore,w_480,h_480,c_fit,q_auto,f_auto/[^"]+',
                script.text,
            )
            cleaned_matches = [
                url.rstrip("\\").replace("l_watermark_346,w_480,h_480/", "")
                for url in matches
            ]
            image_urls.extend(cleaned_matches)
    return list(set(image_urls))  # Removing duplicate images


async def download_image(session, url, file_path):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                f = await aiofiles.open(file_path, mode="wb")
                await f.write(await response.read())
                await f.close()
                return file_path
    except Exception as e:
        print(f"Failed to download {url}. Error: {e}")
        return None


async def save_images(image_urls, med_name):
    file_paths = []
    med_name = med_name.replace(" ", "_").replace(
        "/", "_"
    )  # Replace slashes to avoid path issues
    master_folder = "med_images"
    os.makedirs(master_folder, exist_ok=True)
    med_folder = os.path.join(master_folder, med_name)
    os.makedirs(med_folder, exist_ok=True)

    async with ClientSession() as session:
        tasks = []
        for i, url in enumerate(image_urls):
            file_path = f"{med_folder}/local_image_{i}.jpg"
            if os.path.isfile(file_path):
                print(f"File {file_path} already exists. Skipping download.")
                file_paths.append(file_path)
                continue
            task = asyncio.ensure_future(download_image(session, url, file_path))
            tasks.append(task)

        downloaded_files = await asyncio.gather(*tasks)
        file_paths.extend([file for file in downloaded_files if file])

    return file_paths


async def get_total_pages(session, url):
    content = await fetch(session, url)
    soup = BeautifulSoup(content, "html.parser")
    page_count_soup = soup.find(class_="list-pagination")
    page_links = page_count_soup.find_all("a", class_="button-text link-page")
    last_page_number = int(page_links[-1].text) if page_links else 0
    return last_page_number


def save_processed_url(file_path, url):
    with open(file_path, "a") as file:
        file.write(url + "\n")


def load_processed_urls(file_path):
    try:
        with open(file_path, "r") as file:
            return set(line.strip() for line in file)
    except FileNotFoundError:
        return set()


processed_urls = load_processed_urls("processed_urls.txt")


async def extract_all_info(session, url, file_path='processed_urls.txt'):
    if url in processed_urls:
        logging.info(f"Skipping already processed URL: {url}")
        return None

    content = await fetch(session, url)
    soup = BeautifulSoup(content, "html.parser")
    element = soup.find(class_="DrugHeader__title-content___2ZaPo")
    if element:
        med_name = element.text
    else:
        med_name = "N/A"
        logging.info(f"med_name not found for URL: {url}")

    real_mrp_classes = [
        "DrugPriceBox__slashed-price___2UGqd",
        "PriceBoxPlanOption__margin-right-4___2aqFt PriceBoxPlanOption__stike___pDQVN",
    ]
    real_mrp = None
    for cls in real_mrp_classes:
        real_mrp_element = soup.find(class_=cls)
        if real_mrp_element:
            real_mrp = (
                real_mrp_element.text.replace("₹", "").replace("\xa0", "").strip()
            )
            break
    if real_mrp is None:
        real_mrp = "N/A"
        print(f"Failed for the {url}")

    dis_mrp_classes = [
        "DrugPriceBox__best-price___32JXw",
        "PriceBoxPlanOption__offer-price___3v9x8 PriceBoxPlanOption__offer-price-cp___2QPU_",
    ]
    dis_mrp = None
    for cls in dis_mrp_classes:
        dis_mrp_element = soup.find(class_=cls)
        if dis_mrp_element:
            dis_mrp = dis_mrp_element.text.replace("₹", "").replace("\xa0", "").strip()
            break
    if dis_mrp is None:
        dis_mrp = "N/A"
        print(f"Failed for the {url}")

    # For elements expected to exist once, use a conditional expression
    med_des_element = soup.find(class_="DrugOverview__content___22ZBX")
    med_des = med_des_element.text if med_des_element else "N/A"

    marketer_element = soup.find(class_="DrugHeader__meta-value___vqYM0")
    marketer = marketer_element.text if marketer_element else "N/A"

    salt_comp_element = soup.find(class_="saltInfo DrugHeader__meta-value___vqYM0")
    salt_comp = salt_comp_element.text if salt_comp_element else "N/A"

    # For elements expected to exist multiple times, check the result before accessing
    storage_elements = soup.find_all(class_="saltInfo DrugHeader__meta-value___vqYM0")
    storage = storage_elements[-1].text if storage_elements else "N/A"
    if "N/A" in (med_des, marketer, salt_comp, storage):
        logging.info(f"One or more elements not found for URL: {url}")
    # Assuming get_images and save_images are adapted to be asynchronous
    image_urls = get_images(soup)
    file_paths = await save_images(image_urls, med_name)
    processed_urls.add(url)
    save_processed_url(file_path, url)
    return {
        "medicine_name": med_name,
        "mrp": real_mrp,
        "discount": dis_mrp,
        "marketer": marketer,
        "salt_composition": salt_comp,
        "storage": storage,
        "file_paths": file_paths,
        "about": med_des,
    }


async def extract_page(session, page_url):
    content = await fetch(session, page_url)
    soup = BeautifulSoup(content, "html.parser")

    # Assuming the class for med-items might need adjustment based on actual page structure
    med_items = soup.find_all(
        class_="style__width-100p___2woP5 style__flex-row___m8FHw"
    )
    medicine_info = []

    tasks = [
        extract_all_info(session, "https://www.1mg.com" + item.find("a")["href"])
        for item in med_items
    ]
    results = await asyncio.gather(*tasks)
    medicine_info.extend(results)

    return medicine_info


async def save_to_csv(data, label, batch_id):
    # Filter out None values from data
    filtered_data = [item for item in data if item is not None]
    
    # Proceed only if there's data to save
    if filtered_data:
        df = pd.DataFrame(filtered_data)
        folder_path = "medicine_info"
        os.makedirs(folder_path, exist_ok=True)
        file_path = f"{folder_path}/medicine_info_{label}_{batch_id}.csv"
        df.to_csv(file_path, index=False)

async def process_label(session, label):
    base_url = "https://www.1mg.com/drugs-all-medicines"
    url = f"{base_url}?label={label}"
    total_pages = await get_total_pages(session, url)
    logging.info(f"Processing label '{label}' with {total_pages} pages.")
    all_medicine_info = []
    batch_id = 1

    for page_number in range(1, total_pages + 1):
        page_url = f"{base_url}?label={label}&page={page_number}"
        page_info = await extract_page(session, page_url)
        all_medicine_info.extend(page_info)
        logging.info(f"Label '{label}': Processed page {page_number}/{total_pages}.")
        if len(all_medicine_info) >= 100:
            await save_to_csv(all_medicine_info, label, batch_id)
            all_medicine_info.clear()  # Reset for the next batch
            batch_id += 1

    # Save any remaining data
    if all_medicine_info:
        await save_to_csv(all_medicine_info, label, "final")

def merge_csv_files(label, output_file):
    # Construct the pattern to match CSV files for the label
    pattern = f'medicine_info/medicine_info_{label}_*.csv'
    csv_files = glob.glob(pattern)
    
    # Read and concatenate all matching CSV files into a single DataFrame
    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    
    # Save the merged DataFrame to a new CSV file
    df.to_csv(output_file, index=False)
    print(f"Merged CSV files into {output_file}")

async def main():
    async with aiohttp.ClientSession() as session:
        labels = map(chr, range(ord('b'), ord('d')+1))
        tasks = [process_label(session, label) for label in labels]
        await asyncio.gather(*tasks)
        for label in labels:
            output_file = f'medicine_info/merged_medicine_info_{label}.csv'
            merge_csv_files(label, output_file)


if __name__ == "__main__":
    asyncio.run(main())
