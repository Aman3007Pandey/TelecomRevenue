""" Cleaning logic of data stored in mongo using aggregate pipelines """
from jellyfish import jaro_winkler
from config.mongo_config import mongo_options
from src.utility.mongo_connection import MongoDBConnector


def clean_brand_names(collection_name, db_instance):
    """
    Cleans the 'os_name' field in the MongoDB collection by replacing 'Null'
      values with the most frequent 
    'os_name' value for each unique 'brand_name'. If 'brand_name' is 'Null',
      it replaces 'Null' with the most 
    frequent 'os_name' value across all 'brand_name'.

    Parameters:
    collection_name (str): The name of the MongoDB collection.
    db_instance (pymongo.database.Database): The MongoDB database instance.

    Returns:
    None
    """
    collection = db_instance[collection_name]
    most_frequent_brands = {}

    distinct_brands = collection.distinct("brand_name")
    
    for brand in distinct_brands:
        if brand!= "Null":
            pipeline = [
                {"$match": {"brand_name": brand, "os_name": {"$ne": "Null"}}},
                {"$group": {"_id": "$os_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 1}
            ]
            most_frequent_doc = list(collection.aggregate(pipeline))

            if most_frequent_doc:
                most_frequent_os_name = most_frequent_doc[0]["_id"]
                most_frequent_brands[brand] = most_frequent_os_name

    pipeline_null_brand = [
        {"$match": {"brand_name": "Null", "os_name": {"$ne": "Null"}}},
        {"$group": {"_id": "$os_name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    most_frequent_null_brand = list(collection.aggregate(pipeline_null_brand))
    if most_frequent_null_brand:
        most_frequent_os_name_null_brand = most_frequent_null_brand[0]["_id"]
        most_frequent_brands["Null"] = most_frequent_os_name_null_brand

    for document in collection.find():
        brand = document["brand_name"]
        os_name = document["os_name"]

        if os_name == "Null" and brand in most_frequent_brands:
            most_frequent_os_name = most_frequent_brands[brand]
            collection.update_one({"_id": document["_id"]}, \
                                  {"$set": {"os_name": most_frequent_os_name}})
    print(f"Brand names cleaned in collection '{collection_name}'.")


def clean_os_producers(collection_name, db_instance):
    """
    Cleans the 'os_vendor' field in the MongoDB collection by replacing 'Null'
      values with the most frequent 
    'os_vendor' value for each unique 'os_name'. If 'os_name' is 'Null',
      it replaces 'Null' with the most 
    frequent 'os_vendor' value across all 'os_name'.

    Parameters:
    collection_name (str): The name of the MongoDB collection.
    db_instance (pymongo.database.Database): The MongoDB database instance.

    Returns:
    None
    """
    collection = db_instance[collection_name]
    most_frequent_producers = {}

    # Get distinct os_names
    distinct_os_names = collection.distinct("os_name")
    
    # For each os_name, find the most frequent os_vendor
    for os_name in distinct_os_names:
        pipeline = [
            {"$match": {"os_name": os_name, "os_vendor": {"$ne": "Null"}}},
            {"$group": {"_id": "$os_vendor", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        most_frequent_doc = list(collection.aggregate(pipeline))

        if most_frequent_doc:
            most_frequent_os_vendor = most_frequent_doc[0]["_id"]
            most_frequent_producers[os_name] = most_frequent_os_vendor

    # Replace 'Null' os_vendor with the most frequent os_vendor for each os_name
    for document in collection.find():
        os_name = document["os_name"]
        os_vendor = document["os_vendor"]

        if os_vendor == "Null" and os_name in most_frequent_producers:
            most_frequent_os_vendor = most_frequent_producers[os_name]
            collection.update_one({"_id": document["_id"]}, \
                                  {"$set": {"os_vendor": most_frequent_os_vendor}})

    # Print success message
    print(f"OS producers cleaned in collection '{collection_name}'.")

def clean_gender(gender_value):
    """
    This function cleans the gender value by converting it to lowercase,
      stripping leading/trailing spaces,
    and replacing 'Null' with 'other'. It then checks for the closest match with 
    predefined male and female terms
    using the Jaro-Winkler distance algorithm. If a match is found with a 
    similarity score greater than or equal to 0.85,
    it returns 'ale' or 'female' accordingly. Otherwise, it returns 'other'.

    Parameters:
    gender_value (str): The original gender value to be cleaned.

    Returns:
    str: The cleaned gender value ('male', 'female', or 'other').
    """
    if gender_value == 'Null':
        return 'other'
    
    gender_value = str(gender_value).strip().lower()
    
    male_terms = ['male', '']
    female_terms = ['female', 'f']
    all_terms = male_terms + female_terms
    
    max_similarity = 0
    best_match = None
    for term in all_terms:
        similarity = jaro_winkler(gender_value, term)
        if similarity > max_similarity:
            max_similarity = similarity
            best_match = term
    
    if max_similarity >= 0.85:
        if best_match in male_terms:
            return 'ale'
        elif best_match in female_terms:
            return 'female'
    
    return 'other'

def count_null_values(document):
    """
    This function calculates the number of null or 'Null' values in a given document.

    Parameters:
    document (dict): A dictionary representing a MongoDB document.

    Returns:
    int: The count of null or 'Null' values in the document.
    """
    null_count = 0
    for key, value in document.items():
        if value is None or value == "Null":
            null_count += 1
    return null_count

def add_null_count_field(collection):
    """
    This function iterates over all documents in the given MongoDB collection,
      calculates the number of null or 'Null' 
    values in each document using the count_null_values function, and updates
      the document with the calculated null_count.

    Parameters:
    collection (pymongo.collection.Collection): The MongoDB collection to process.

    Returns:
    None
    """
    for document in collection.find({}):
        null_count = count_null_values(document)
        collection.update_one(
            {"_id": document["_id"]},
            {"$set": {"null_count": null_count}},
        )
    
def null_add_and_msisdn_unique(collection, session, db_instance):
    """
    This function performs several operations on a MongoDB collection:
    1. Adds a new field 'null_count' to each document, which represents
      the count of null or 'Null' values in the document.
    2. Sorts the documents in ascending order of 'sisdn', 'null_count',
      'ystem_status', and 'gender'.
    3. Assigns a rank to each document within each 'sisdn' group based on the sorted order.
    4. Retains only the documents with rank 1.
    5. Drops the original collection and renames the temporary collection
      to the original name.

    Parameters:
    collection (pymongo.collection.Collection): The MongoDB collection to process.
    session (pymongo.client_session.ClientSession): The MongoDB client 
    session for executing the operations.
    db_instance (pymongo.database.Database): The MongoDB database instance.

    Returns:
    None
    """
    pipeline = [
        {"$addFields": {
            "null_count": {
                "$size": {
                    "$filter": {
                        "input": {"$objectToArray": "$$ROOT"},
                        "as": "kv",
                        "cond": {"$or": [{"$eq": ["$$kv.v", None]},\
                                          {"$eq": ["$$kv.v", "Null"]}]}
                    }
                }
            }
        }},

        {"$sort": {
            "msisdn": 1,
            "null_count": 1,
            "system_status": -1,
            "gender": -1
        }},

        {"$setWindowFields": {
            "partitionBy": "$msisdn",
            "sortBy": {
                "null_count": 1,
                "system_status": -1,
                "gender": -1
            },
            "output": {
                "rank": {
                    "$rank": {}
                }
            }
        }},

        {"$match": {
            "rank": 1
        }},

        {"$unset": "rank"}
    ]
    result = list(collection.aggregate(pipeline, session=session))

    temp_collection = db_instance["temp_crm1"]
    temp_collection.drop()
    if result:
        temp_collection.insert_many(result, session=session)

    collection.drop()
    temp_collection.rename("crm1", session=session)
    
def test_collection_conditions(collection):
    """
    This function tests the conditions of a MongoDB collection.

    Parameters:
    collection (pymongo.collection.Collection): The MongoDB collection to test.

    Returns:
    bool: True if all tests pass, False otherwise.
    """

    # Test for duplicate msisdns
    pipeline_test = [
        {"$group": {
            "_id": "$msisdn",
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    duplicates = list(collection.aggregate(pipeline_test))
    if duplicates:
        print(f"Test failed: Found duplicate msisdns: {duplicates}")
        return False
    else:
        print("Test passed: No duplicate msisdns found.")

    # Test for correct computation of null_count
    sample_document = collection.find_one()
    if sample_document:
        null_count_correct = sample_document["null_count"] \
        == sum(1 for v in sample_document.values() if v is None)
        if null_count_correct:
            print("Test passed: null_count computed correctly.")
        else:
            print("Test failed: null_count not computed correctly.")

    # Test for each document having the highest system_status and gender within its
    # msisdn group
    for doc in collection.find():
        msisdn = doc["msisdn"]
        highest_doc = collection.find({"msisdn": msisdn}).sort([("system_status", -1),\
                                                                 ("gender", -1)]).limit(1)
        if highest_doc[0]["_id"]!= doc["_id"]:
            print(f"Test failed: Document with msisdn {msisdn} \
                   does not have the highest system_status and gender.")
            return False
            break
    else:
        print("Test passed: Each document has the highest system_status \
               and gender within its msisdn group.")
        return True

def main():
    # Initialize MongoDB connector
    mongo_connector = MongoDBConnector()
    db_instance = mongo_connector.db  

    # Clean brand names and OS producers
    clean_brand_names(mongo_options["DEVICE_COLLECTION_NAME"], db_instance)
    clean_os_producers(mongo_options["DEVICE_COLLECTION_NAME"], db_instance)  

    # Execute tests
    collection = mongo_options["CRM_COLLECTION_NAME"]
    test_collection_conditions(collection)

if __name__ == "__main__":
    main()
