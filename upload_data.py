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

class UploadToDB:
    def __init__(self):
        self.setup_logging()
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
        print("Connected to S3")

    def setup_session(self):
        self.session = requests.Session()
        self.session.headers.update({
            'accept': '*/*'
        })

    def upload_image_chunk(self, image_paths: List[str]) -> List[str]:
        try:
            files = []
            print(f"Uploading {len(image_paths)} images to s3")
            
            if len(image_paths) <= 1: 
                urls = ['https://medinos-backend.s3.amazonaws.com/uploads/1729321881992_medicine.png']
                return urls

            for path in image_paths:
                adjusted_path = Path('..').joinpath(path).resolve()
                if os.path.exists(adjusted_path):
                    file_name = os.path.basename(adjusted_path)
                    files.append(
                        ('files', (file_name, open(adjusted_path, 'rb'), 'image/jpeg'))
                    )
                else:
                    logging.warning(f"File not found: {path}")
            print(files)
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
            print(f"image_chunks: {image_chunks}")

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
            image_paths = med_data['file_paths']
            print(image_paths)
            if isinstance(image_paths, str):
                image_paths = eval(image_paths)
                print(image_paths)

            image_urls = self.upload_image_via_api(image_paths, med_data['medicine_name'])
            print(image_urls)

            print('image url')
            if not image_urls:
                logging.warning(f"No image uploaded for {med_data['medicine_name']}")
            
            category_id = 'cm2fsk7130000du76q8wodi6y'
            item_code = '0'
            hsn_code = '0'
            packaging_size = '0'
            packaging_size_id = 'cm1c0iybi00003j6gnuanb0by'

            print(med_data['marketer'])
            marketer = med_data.get('marketer')

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
                INSERT INTO products (id, item_name, category_id, hsn_code, item_code, item_salt, price, discount, description, image_url, manufacturer_id, packaging_size, packaging_size_id, is_prescription_required)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id """, (
                    product_id,
                    med_data['medicine_name'],
                    category_id,
                    hsn_code,
                    item_code,
                    med_data['salt_composition'],
                    float(med_data['mrp']),
                    float(med_data['discount']),
                    med_data['about'],
                    image_urls,
                    manufacturer_id,
                    packaging_size,
                    packaging_size_id,
                    med_data['is_prescription_required']
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

            for index, row in df.iterrows():
                logging.info(f"Processing product {index + 1} of {total_products}")
                product_id = self.insert_product(row.to_dict())

                if product_id:
                    successful_uploads +=1
                    logging.info(f"Progress: {successful_uploads}/{total_products} products processed")
            
            logging.info(f"Completed processing {csv_file}. Successfully uploaded {successful_uploads} out of {total_products} products")

        except Exception as e:
            logging.error(f"Error processing {csv_file}: {str(e)}")

    def run(self):
        try:
            csv_dir = Path('../medicine_info')
            csv_files = list(csv_dir.glob('*.csv'))

            for csv_file in csv_files:
                self.process_csv_file(str(csv_file))
            
        finally:
            self.db_conn.close()
            print("Connection closed")

if __name__ == "__main__":
    uploader = UploadToDB()
    uploader.run()
