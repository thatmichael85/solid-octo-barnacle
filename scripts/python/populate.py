import random
import string
import time
from pymongo import MongoClient
from bson import BSON
from concurrent.futures import ThreadPoolExecutor
from threading import Event

def generate_random_data(size=1024):
    """Generate a random string of specified size in bytes."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def create_index(mongo_client, db_name, collection_name):
    db = mongo_client[db_name]
    collection = db[collection_name]
    # Creating an index on the 'name' field
    print(f'Creating index for {collection_name}')
    collection.create_index("name")

def insert_batch(mongo_client, db_name, collection_name, batch_data, names, stop_event):
    db = mongo_client[db_name]
    collection = db[collection_name]

    batch = [{'data': data, 'name': name, 'timeStamp': int(time.time() * 1000)} for data, name in zip(batch_data, names)]
    result = collection.insert_many(batch)
    return len(result.inserted_ids) * len(BSON.encode(batch[0]))

def insert_data_into_collection(collection_name, stop_event):
    client_url = 'mongodb://localhost:27017/'
    mongo_client = MongoClient(client_url)
    db_name = 'DefaultDatabase'
    target_size = 3 * 1024 * 1024 * 1024  # GB in bytes
    document_size = 1024 * 10  # Size of each document in bytes (approximate)
    batch_size = 500  # Number of documents to insert at once
    total_size = 0

    # Create an index on the 'name' field before inserting data
    create_index(mongo_client, db_name, collection_name)

    while total_size < target_size and not stop_event.is_set():
        batch_data = [generate_random_data(document_size) for _ in range(batch_size)]
        names = ['Name_' + str(i) for i in range(len(batch_data))] 
        total_inserted = insert_batch(mongo_client, db_name, collection_name, batch_data, names, stop_event)
        total_size += total_inserted
        print(f"Inserted batch of size {total_inserted/10**6} Mbytes into {collection_name}, total size: {total_size/10**6} Mbytes")
    
    print(f"Insertion into {collection_name} stopped or completed.")

def main():
    collection_names = ['yourCollectionName', 'Collection2', 'Collection3', 'AnotherOne']  # Collection names
    stop_event = Event()

    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for collection_name in collection_names:
                executor.submit(insert_data_into_collection, collection_name, stop_event)
    except KeyboardInterrupt:
        print("Received interrupt, stopping threads...")
        stop_event.set() 

    print("Finished or stopped populating the MongoDB collections.")

if __name__ == "__main__":
    main()
