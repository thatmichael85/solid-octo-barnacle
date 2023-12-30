import random
import string
import time
from pymongo import MongoClient
from bson import BSON
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

def generate_random_data(size=1024):
    """Generate a random string of specified size in bytes."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def insert_batch(client_url, db_name, collection_name, batch_data, stop_event):
    client = MongoClient(client_url)
    db = client[db_name]
    collection = db[collection_name]

    if stop_event.is_set():
        return 0  # Early exit if stop event is set

    batch = [{'data': data} for data in batch_data]  # Convert data to documents
    result = collection.insert_many(batch)
    return len(result.inserted_ids) * len(BSON.encode(batch[0]))

def main():
    client_url = 'mongodb://sourceUsername:sourcePassword@127.0.0.1:27017/'
    db_name = 'DefaultDatabase'
    collection_name = 'yourCollectionName'

    total_size = 0
    target_size = 25 * 1024 * 1024 * 1024  # 9 GB in bytes
    document_size = 1024 * 10  # Size of each document in bytes (approximate)
    batch_size = 10000  # Number of documents to insert at once
    max_workers = 5  # Number of threads for parallel insertion
    stop_event = Event()  # Event to signal stop

    start_time = time.time()  # Start timing

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        try:
            while total_size < target_size and not stop_event.is_set():
                batch_data = [generate_random_data(document_size) for _ in range(batch_size)]
                future = executor.submit(insert_batch, client_url, db_name, collection_name, batch_data, stop_event)
                futures.append(future)

                # Check if any future is completed and update total_size
                for future in as_completed(futures):
                    total_inserted = future.result()
                    total_size += total_inserted
                    print(f"Inserted batch of size {total_inserted} bytes, total size: {total_size} bytes")
        except KeyboardInterrupt:
            stop_event.set()
            print("Stopping insertion process...")

    if batch_data:
        remaining_inserted = insert_batch(client_url, db_name, collection_name, batch_data, stop_event)
        total_size += remaining_inserted
        print(f"Inserted final batch of size {remaining_inserted} bytes, total size: {total_size} bytes")

    end_time = time.time()  # End timing

    print("Finished populating the MongoDB collection.")
    print(f"Total time taken: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
