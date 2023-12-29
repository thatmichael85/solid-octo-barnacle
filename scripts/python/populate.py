import random
import string
import time
from pymongo import MongoClient
from bson import BSON

def generate_random_data(size=1024):
    """Generate a random string of specified size in bytes."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def main():
    client = MongoClient('mongodb://sourceUsername:sourcePassword@127.0.0.1:27017/')
    db = client['DefaultDatabase']
    collection = db['yourCollectionName']

    total_size = 0
    target_size = 1 * 1024 * 1024 #* 1024  # 1 GB in bytes
    document_size = 1024 * 10  # Size of each document in bytes (approximate)
    batch_size = 5000  # Number of documents to insert at once
    batch = []  # Batch of documents

    start_time = time.time()  # Start timing

    while total_size < target_size:
        data = generate_random_data(document_size)
        doc = {'data': data}
        doc_size = len(BSON.encode(doc))
        batch.append(doc)

        if len(batch) >= batch_size:
            collection.insert_many(batch)
            total_inserted = len(batch) * doc_size
            total_size += total_inserted
            print(f"Inserted batch of size {total_inserted} bytes, total size: {total_size} bytes")
            batch.clear()

    if batch:
        collection.insert_many(batch)
        total_inserted = len(batch) * doc_size
        total_size += total_inserted
        print(f"Inserted final batch of size {total_inserted} bytes, total size: {total_size} bytes")

    end_time = time.time()  # End timing

    print("Finished populating the MongoDB collection.")
    print(f"Total time taken: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
