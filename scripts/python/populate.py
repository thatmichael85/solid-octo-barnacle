import random
import string
import time
from pymongo import MongoClient
from bson import BSON
from threading import Event

def generate_random_data(size=1024):
    """Generate a random string of specified size in bytes."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def insert_batch(mongo_client, db_name, collection_name, batch_data, stop_event):
    db = mongo_client[db_name]
    collection = db[collection_name]

    if stop_event.is_set():
        return 0  # Early exit if stop event is set

    batch = [{'data': data} for data in batch_data]  # Convert data to documents
    result = collection.insert_many(batch)
    return len(result.inserted_ids) * len(BSON.encode(batch[0]))

def main():
    client_url = 'mongodb://localhost:27017/'
    mongo_client = MongoClient(client_url)  # Create a single MongoClient instance
    db_name = 'DefaultDatabase'
    collection_name = 'yourCollectionName'

    total_size = 0
    target_size = 25 * 1024 * 1024 * 1024  # GB in bytes
    document_size = 1024 * 10  # Size of each document in bytes (approximate)
    batch_size = 500  # Number of documents to insert at once
    stop_event = Event()  # Event to signal stop

    start_time = time.time()  # Start timing

    try:
        while total_size < target_size and not stop_event.is_set():
            batch_data = [generate_random_data(document_size) for _ in range(batch_size)]
            total_inserted = insert_batch(mongo_client, db_name, collection_name, batch_data, stop_event)
            total_size += total_inserted
            print(f"Inserted batch of size {total_inserted/10**6} Mbytes, total size: {total_size/10**6} Mbytes")
    except KeyboardInterrupt:
        stop_event.set()
        print("Stopping insertion process...")

    if batch_data:
        remaining_inserted = insert_batch(mongo_client, db_name, collection_name, batch_data, stop_event)
        total_size += remaining_inserted
        print(f"Inserted final batch of size {remaining_inserted/10**6} Mbytes, total size: {total_size/10**6} Mbytes")

    end_time = time.time()  # End timing

    print("Finished populating the MongoDB collection.")
    print(f"Total time taken: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
