import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import os
import glob
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import aiofiles
from aiohttp import ClientSession
import re
import logging
from typing import Dict, List, Optional
from datetime import datetime
import time
from pathlib import Path

class MedicineScraper:
    def __init__(self):
        self.setup_logging()
        self.setup_directories()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }
        self.processed_urls = self.load_processed_urls()
        self.session_timeout = aiohttp.ClientTimeout(total=30)
        self.retry_delay = 2
        self.max_retries = 3
        self.concurrent_requests = 3

    def setup_logging(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"scraper_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        logging.getLogger("aiohttp.client").setLevel(logging.ERROR)

    def setup_directories(self):
        directories = ['med_images', 'medicine_info', 'logs']
        for directory in directories:
            Path(directory).mkdir(exist_ok=True)

    def load_processed_urls(self) -> set:
        try:
            with open("processed_urls.txt", "r") as file:
                return set(line.strip() for line in file)
        except FileNotFoundError:
            return set()

    async def fetch_with_retry(self, session: ClientSession, url: str) -> Optional[str]:
        for attempt in range(self.max_retries):
            try:
                async with session.get(url, headers=self.headers, timeout=self.session_timeout) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        wait_time = int(response.headers.get('Retry-After', self.retry_delay * (attempt + 1)))
                        logging.warning(f"Rate limited. Waiting {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.error(f"HTTP {response.status} for {url}")
                        await asyncio.sleep(self.retry_delay * (attempt + 1))
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    return None
        return None

    def extract_price(self, soup: BeautifulSoup, classes: List[str]) -> str:
        for cls in classes:
            element = soup.find(class_=cls)
            if element:
                return element.text.replace("₹", "").replace("\xa0", "").strip()
        return "N/A"

    def get_images(self, soup: BeautifulSoup) -> List[str]:
        image_urls = []
        for script in soup.find_all("script"):
            if "window.PRELOADED_STATE" in script.text:
                matches = re.findall(
                    r'https://onemg.gumlet.io/[^"]+',
                    script.text
                )
                image_urls.extend([url.rstrip("\\") for url in matches])
        return list(set(image_urls))

    async def download_image(self, session: ClientSession, url: str, file_path: str) -> Optional[str]:
        try:
            async with session.get(url, timeout=self.session_timeout) as response:
                if response.status == 200:
                    async with aiofiles.open(file_path, mode='wb') as f:
                        await f.write(await response.read())
                    return file_path
        except Exception as e:
            logging.error(f"Failed to download {url}: {str(e)}")
        return None

    async def save_images(self, session: ClientSession, image_urls: List[str], med_name: str) -> List[str]:
        med_folder = Path("med_images") / med_name.replace(" ", "_").replace("/", "_")
        med_folder.mkdir(parents=True, exist_ok=True)

        tasks = []
        file_paths = []
        
        for i, url in enumerate(image_urls):
            file_path = med_folder / f"image_{i}.jpg"
            if file_path.exists():
                file_paths.append(str(file_path))
                continue
            tasks.append(self.download_image(session, url, str(file_path)))

        if tasks:
            results = await asyncio.gather(*tasks)
            file_paths.extend([r for r in results if r])
        
        return file_paths

    async def extract_medicine_info(self, session: ClientSession, url: str) -> Optional[Dict]:
        if url in self.processed_urls:
            return None

        content = await self.fetch_with_retry(session, url)
        if not content:
            return None

        soup = BeautifulSoup(content, 'html.parser')
        
        try:
            info = {
                'medicine_name': soup.find(class_="DrugHeader__title-content___2ZaPo").text.strip(),
                'mrp': self.extract_price(soup, ["DrugPriceBox__slashed-price___2UGqd"]),
                'discount': self.extract_price(soup, ["DrugPriceBox__best-price___32JXw"]),
                'marketer': next((elem.text.strip() for elem in soup.find_all(class_="DrugHeader__meta-value___vqYM0")), "N/A"),
                'salt_composition': soup.find(class_="saltInfo DrugHeader__meta-value___vqYM0").text.strip(),
                'about': soup.find(class_="DrugOverview__content___22ZBX").text.strip()
            }

            image_urls = self.get_images(soup)
            info['image_paths'] = await self.save_images(session, image_urls, info['medicine_name'])
            
            self.processed_urls.add(url)
            async with aiofiles.open("processed_urls.txt", "a") as f:
                await f.write(f"{url}\n")
            
            return info

        except Exception as e:
            logging.error(f"Error extracting info from {url}: {str(e)}")
            return None

    async def process_page(self, session: ClientSession, page_url: str) -> List[Dict]:
        content = await self.fetch_with_retry(session, page_url)
        if not content:
            return []

        soup = BeautifulSoup(content, 'html.parser')
        med_items = soup.find_all(class_="style__width-100p___2woP5 style__flex-row___m8FHw")
        
        tasks = []
        for item in med_items:
            link = item.find("a")
            if link and 'href' in link.attrs:
                url = f"https://www.1mg.com{link['href']}"
                tasks.append(self.extract_medicine_info(session, url))

        results = await asyncio.gather(*tasks)
        return [r for r in results if r]

    async def process_label(self, session: ClientSession, label: str):
        base_url = f"https://www.1mg.com/drugs-all-medicines?label={label}"
        
        try:
            content = await self.fetch_with_retry(session, base_url)
            if not content:
                return

            soup = BeautifulSoup(content, 'html.parser')
            pagination = soup.find(class_="list-pagination")
            if not pagination:
                return

            total_pages = max(int(link.text) for link in pagination.find_all("a", class_="button-text link-page") if link.text.isdigit())
            
            for page in range(1, total_pages + 1):
                page_url = f"{base_url}&page={page}"
                medicines = await self.process_page(session, page_url)
                
                if medicines:
                    df = pd.DataFrame(medicines)
                    output_file = f"medicine_info/medicines_{label}_page_{page}.csv"
                    df.to_csv(output_file, index=False)
                    logging.info(f"Saved {len(medicines)} medicines from {page_url}")
                
                await asyncio.sleep(1)  # Polite delay between pages

        except Exception as e:
            logging.error(f"Error processing label {label}: {str(e)}")

    async def run(self):
        async with aiohttp.ClientSession() as session:
            labels = list(map(chr, range(ord('a'), ord('z')+1)))
            for label in labels:
                await self.process_label(session, label)
                await asyncio.sleep(2)  # Polite delay between labels

if __name__ == "__main__":
    scraper = MedicineScraper()
    asyncio.run(scraper.run())
