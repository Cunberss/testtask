from src.db.mongo_client import collection
from datetime import datetime
import pandas as pd


def aggregate_salary_data(dt_from: str, dt_upto: str, group_type: str):
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    match_stage = {"$match":
                       {"dt": {"$gte": dt_from, "$lte": dt_upto}}
                   }

    project_stage = {"$project":
        {
            "_id": 0,
            "value": 1,
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"},
            "hour": {"$hour": "$dt"}
        }
    }

    if group_type == "hour":
        group_stage = {"$group":
            {
                "_id": {"year": "$year", "month": "$month", "day": "$day", "hour": "$hour"},
                "total": {"$sum": "$value"}
            }
        }
    elif group_type == "day":
        group_stage = {"$group":
            {
                "_id": {"year": "$year", "month": "$month", "day": "$day"},
                "total": {"$sum": "$value"}
            }
        }
    elif group_type == "month":
        group_stage = {"$group":
            {
                "_id": {"year": "$year", "month": "$month"},
                "total": {"$sum": "$value"}
            }
        }

    sort_stage = {"$sort": {"_id": 1}}

    pipeline = [match_stage, project_stage, group_stage, sort_stage]

    result = collection.aggregate(pipeline)

    dataset = []
    labels = []

    for doc in result:
        year = doc["_id"]["year"]
        month = doc["_id"]["month"]
        day = doc["_id"].get("day", 1)
        hour = doc["_id"].get("hour", 0)
        labels.append(f"{year}-{month:02d}-{day:02d}T{hour:02d}:00:00")
        dataset.append(doc["total"])

    print(dataset, labels)
    data = fill_missing_dates(dataset, labels, dt_from, dt_upto, group_type)
    answer = {"dataset": data[0], "labels": data[1]}
    return str(answer)


def fill_missing_dates(dataset, labels, start_date, end_date, group_type):
    labels = pd.to_datetime(labels).to_period('M') if group_type == 'month' else pd.to_datetime(labels)
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    series = pd.Series(dataset, index=labels)
    freq = 'D' if group_type == 'day' else 'M' if group_type == 'month' else 'H'
    index = pd.date_range(start=start_date, end=end_date, freq=freq)
    if group_type == 'month':
        index = index.to_period('M')
    series = series.reindex(index, fill_value=0)
    return series.values.tolist(), series.index.strftime('%Y-%m-%dT%H:%M:%S').tolist()
