import asyncio
import concurrent.futures

from src.db.mongo_client import collection
from datetime import datetime
import pandas as pd


async def aggregate_salary_data(dt_from: str, dt_upto: str, group_type: str):
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

    async for doc in result:
        year = doc["_id"]["year"]
        month = doc["_id"]["month"]
        day = doc["_id"].get("day", 1)
        hour = doc["_id"].get("hour", 0)
        labels.append(f"{year}-{month:02d}-{day:02d}T{hour:02d}:00:00")
        dataset.append(doc["total"])

    loop = asyncio.get_running_loop()

    with concurrent.futures.ThreadPoolExecutor() as pool:
        data = await loop.run_in_executor(pool, fill_missing_dates, dataset, labels, dt_from, dt_upto, group_type)

    answer = {"dataset": data[0], "labels": data[1]}
    return str(answer)


def fill_missing_dates(number_list, date_list, start_date, end_date, group_format):
    date_list = pd.to_datetime(date_list)

    if group_format == 'hour':
        full_dates = pd.date_range(start=start_date, end=end_date, freq='H')
        anchor = 'H'
    elif group_format == 'day':
        full_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        anchor = 'D'
    elif group_format == 'month':
        full_dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' Stands for Month Start frequency
        anchor = 'MS'

    df_numbers = pd.DataFrame({'Date': date_list, 'Number': number_list}).set_index('Date').resample(
        anchor).first().reset_index()
    df_dates = pd.DataFrame({'Date': full_dates})

    merged_df = pd.merge(df_dates, df_numbers, on='Date', how='left').fillna(0)

    return list(map(int,(merged_df['Number']))), list(merged_df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%S'))