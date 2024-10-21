import pandas as pd
import os
import logging
import psycopg2
import boto3
import requests
from pathlib import Path
from psycopg2.extras import execute_batch
from typing import List, Dict
from config import config
from concurrent.futures import ThreadPoolExecutor, as_completed
import cuid
from datetime import datetime

class UploadToDB:
    def __init__(self):
        self.setup_logging()
        self.setup_directories()
        self.setup_connections()
        self.api_base_url = "https://taskar-api.medinos.in"
        self.setup_session()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler('data_upload.log'), logging.StreamHandler()]
        )

    def setup_connections(self):
        self.db_conn = psycopg2.connect(**config['database'])
        print("Connected to database")

        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config['aws']['aws_access_key_id'],
            aws_secret_access_key=config['aws']['aws_secret_access_key'],
            region_name=config['aws']['region_name']
        )
    
    def setup_directories(self):
        self.processed_files_path = 'processed_files.txt'

    def load_processed_files(self) -> set:
        try:
            with open(self.processed_files_path, 'r') as f:
                return set(line.strip() for line in f)
        except FileNotFoundError:
            return set()
    
    def mark_file_as_processed(self, filename: str):
        with open(self.processed_files_path, 'a') as f:
            f.write(f"{filename}\n")

    def setup_session(self):
        self.session = requests.Session()
        self.session.headers.update({
            'accept': '*/*'
        })

    def upload_image_chunk(self, image_paths: List[str]) -> List[str]:
        try:
            files = []
            
            if len(image_paths) <= 1: 
                urls = ['https://medinos-backend.s3.amazonaws.com/uploads/1729321881992_medicine.png']
                return urls

            for path in image_paths:
                # adjusted_path = Path('..').joinpath(path).resolve()
                if os.path.exists(path):
                    file_name = os.path.basename(path)
                    files.append(
                        ('files', (file_name, open(path, 'rb'), 'image/jpeg'))
                    )
                else:
                    logging.warning(f"File not found: {path}")
            response = self.session.post(
                f"{self.api_base_url}/api/aws-s3/upload/multiple_files?type=image",
                files=files
            )

            for file_tuple in files:
                file_tuple[1][1].close()

            if response.status_code == 201:
                result = response.json()
                urls = [item['fileUrl'] for item in result if item['httpStatusCode'] == 200]

                for item in result:
                    logging.info(f"File upload success - RequestId: {item['requestId']}, URL: {item['fileUrl']}")

                return urls
            else:
                logging.error(f"Upload failed with status {response.status_code}: {response.text}")
                return []

        except Exception as e:
            logging.error(f"Error uploading image chunk: {str(e)}")
            return []

        finally:
            for file_tuple in files:
                try:
                    file_tuple[1][1].close()
                except:
                    pass
    
    def upload_image_via_api(self, image_paths: List[str], med_name: str) -> List[str]:
        try:
            chunk_size = 5
            image_chunks = [image_paths[i:i + chunk_size] for i in range(0, len(image_paths), chunk_size) ]
            all_urls = []

            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_chunk = {
                    executor.submit(self.upload_image_chunk, chunk): chunk
                    for chunk in image_chunks
                }

                for future in as_completed(future_to_chunk):
                    try:
                        urls = future.result()
                        all_urls.extend(urls)
                    except Exception as e:
                        logging.error(f"Error processing chunk for {med_name}: {str(e)} ")
            
            logging.info(f"Succesfully uploaded {len(all_urls)} images for { med_name }")
            return all_urls
        
        except Exception as e:
            logging.error(f"Error in upload_image_via_api for {med_name}: {str(e)}")
            return []
    
    def insert_product(self, med_data: Dict) -> int:
        cursor = self.db_conn.cursor()

        try:
            image_paths = med_data['image_paths']
            if isinstance(image_paths, str):
                image_paths = eval(image_paths)

            image_urls = self.upload_image_via_api(image_paths, med_data['medicine_name'])

            if not image_urls:
                logging.warning(f"No image uploaded for {med_data['medicine_name']}")

            category_id = 21
            size = '1'
            marketer = med_data.get('marketer')
            price = med_data.get('mrp')
            discount = med_data.get('discount')
            packaging_type = 'Box'
            stock = 0

            if price == 'N/A' or pd.isna(price) or price in None:
                price = '0'
            else:
                price = float(price)

            if discount == 'N/A' or pd.isna(discount) or discount in None:
                discount = '0'
            else:
                discount = float(discount)
    
            id = cuid.cuid()
            cursor.execute("""INSERT INTO "SizeGroups" (id) VALUES (%s) RETURNING id""", (id,))
            sizeGroupId = cursor.fetchone()[0]

            cursor.execute("SELECT * FROM manufacturers WHERE name= %s", ([marketer]))
            manufacturer_row = cursor.fetchone()

            if manufacturer_row:
                manufacturer_id = manufacturer_row[0]
            else:
                manufacturer_id = cuid.cuid()
                cursor.execute("""INSERT INTO manufacturers (id, name, address, email, "gstNo", "phoneNumber") VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""", (manufacturer_id, marketer, 'NA', 'NA', 'NA', 'NA'))
                manufacturer_id = cursor.fetchone()[0]
            product_id = cuid.cuid()
    
            cursor.execute("""
                INSERT INTO products ("itemCode", name, "categoryId", price, discount, description, "imageUrl", "manufacturerId", size, "sizeGroupId", "isPrescriptionRequired", "packagingType", stock, "updatedAt")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING "itemCode" """, (
                    product_id,
                    med_data['medicine_name'],
                    category_id,
                    price,
                    discount,
                    med_data['about'],
                    image_urls,
                    manufacturer_id,
                    size,
                    sizeGroupId,
                    med_data['is_prescription_required'],
                    packaging_type,
                    stock,
                    datetime.now()
                ))

            product_id = cursor.fetchone()[0]
            self.db_conn.commit()

            logging.info(f"Successfully inserted product {med_data['medicine_name']} with Id {product_id}")
            return product_id
        except Exception as e: 
            self.db_conn.rollback()
            logging.error(f"Error inserting product {med_data['medicine_name']}: {str(e)}")
            return None
        finally:
            cursor.close()

    def process_csv_file(self, csv_file: str):
        try:
            df = pd.read_csv(csv_file)
            total_products = len(df)
            successful_uploads = 0
            failed_products = []

            for index, row in df.iterrows():
                logging.info(f"Processing product {index + 1} of {total_products}")
                try:
                    product_id = self.insert_product(row.to_dict())

                    if product_id:
                        successful_uploads +=1
                        logging.info(f"Progress: {successful_uploads}/{total_products} products processed")
                    else:
                        failed_products.append(row['medicine_name'])
                except Exception as e:
                    failed_products.append(row['medicine_name'])
                    logging.error(f"Error processing product {row['medicine_name']}: {str(e)}")
                    continue
            
            logging.info(f"Completed processing {csv_file}") 
            logging.info(f"Successfully uploaded {successful_uploads} out of {total_products} products")

            if failed_products:
                failed_file = f"failed_{os.path.basename(csv_file)}"
                df[df['medicine_name'].isin(failed_products)].to_csv(failed_file, index=False)
                logging.info(f"Failed products saved to {failed_file}")

            if successful_uploads == total_products:
                return True
            return False

        except Exception as e:
            logging.error(f"Error processing {csv_file}: {str(e)}")
            return False

    def run(self):
        try:
            csv_dir = Path('medicine_info')
            csv_files = list(csv_dir.glob('*.csv'))
            processed_files = self.load_processed_files()

            for csv_file in csv_files:
                csv_path = str(csv_file)
                if csv_path in processed_files:
                    logging.info(f"Skipping {csv_file} as already processed")
                    continue

                logging.info(f"Processing new file {csv_path}")
                try:
                    self.process_csv_file(csv_path)
                    self.mark_file_as_processed(csv_path)
                    logging.info(f"Marked {csv_path} as processed")
                except Exception as e:
                    logging.error(f"Error processing {csv_file}: {str(e)}")
                    continue
            
        finally:
            self.db_conn.close()
            print("Connection closed")

if __name__ == "__main__":
    uploader = UploadToDB()
    uploader.run()
