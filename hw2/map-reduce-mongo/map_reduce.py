from __future__ import annotations

import json
from pathlib import Path

from bson.code import Code
from pymongo import MongoClient

DATASET_PATH = Path(__file__).parent / "data" / "orders.json"
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DATABASE_NAME = "commerce_db"
SOURCE_COLLECTION = "orders"
RESULT_COLLECTION = "category_sales"


def load_dataset(collection):
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATASET_PATH}")

    collection.drop()

    documents = []
    with DATASET_PATH.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            documents.append(json.loads(line))

    if not documents:
        raise ValueError("Dataset is empty; nothing to insert")

    collection.insert_many(documents)


def create_map_function():
    return Code(
        """
        function () {
            if (!this.items) {
                return;
            }
            this.items.forEach(function (item) {
                if (!item.category || !item.price) {
                    return;
                }
                emit(item.category, { count: 1, total: item.price });
            });
        }
        """
    )


def create_reduce_function():
    return Code(
        """
        function (key, values) {
            var reduced = { count: 0, total: 0 };
            values.forEach(function (value) {
                reduced.count += value.count;
                reduced.total += value.total;
            });
            return reduced;
        }
        """
    )


def create_finalize_function():
    return Code(
        """
        function (key, reduced) {
            reduced.avg = reduced.total / reduced.count;
            return reduced;
        }
        """
    )


def execute_category_sales_mapreduce(db):
    map_function = create_map_function()
    reduce_function = create_reduce_function()
    finalize_function = create_finalize_function()

    db[RESULT_COLLECTION].drop()

    db.command(
        {
            "mapReduce": SOURCE_COLLECTION,
            "map": map_function,
            "reduce": reduce_function,
            "out": {"replace": RESULT_COLLECTION},
            "finalize": finalize_function,
        }
    )


def execute_category_sales_aggregation(collection, result_collection):
    result_collection.drop()
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.category",
                "count": {"$sum": 1},
                "total": {"$sum": "$items.price"}
            }
        },
        {
            "$addFields": {
                "avg": {"$divide": ["$total", "$count"]}
            }
        },
        {"$sort": {"_id": 1}},
        {"$out": RESULT_COLLECTION}
    ]

    collection.aggregate(pipeline)


def display_category_sales_results(db):
    for doc in db[RESULT_COLLECTION].find().sort("_id", 1):
        value = doc.get("value")
        if value is None:
            value = {
                "count": doc.get("count"),
                "total": doc.get("total"),
                "avg": doc.get("avg"),
            }

        if value["count"] is None or value["total"] is None or value["avg"] is None:
            raise ValueError(
                f"Result document for category '{doc['_id']}' is missing expected fields."
            )

        print(
            f"{doc['_id']}: count={value['count']}, total={value['total']:.2f}, avg={value['avg']:.2f}"
        )


def main():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DATABASE_NAME]
    source_collection = db[SOURCE_COLLECTION]
    result_collection = db[RESULT_COLLECTION]

    load_dataset(source_collection)

    print("Results using mapReduce (depricated):")
    execute_category_sales_mapreduce(db)
    display_category_sales_results(db)

    print("\n" + "=" * 50 + "\n")

    print("Results using aggregation pipeline (Latest Alternative):")
    execute_category_sales_aggregation(source_collection, result_collection)
    display_category_sales_results(db)


if __name__ == "__main__":
    main()
