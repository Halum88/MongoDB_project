import pymongo
from datetime import datetime, timedelta
import calendar

async def get_aggreg_data(dt_from, dt_upto, group_type):
    
    client = pymongo.MongoClient('localhost', 27017)
    db = client['sampleDB']
    series_collection = db['sample_collection']
    
    dt_from = datetime.strptime(dt_from, "%Y-%m-%dT%H:%M:%S")
    dt_upto = datetime.strptime(dt_upto, "%Y-%m-%dT%H:%M:%S")


    request = [
            {'$match': {'dt': {'$gte': dt_from, '$lte': dt_upto}}},
            {'$group': {
                '_id': {'$dateTrunc': {'date': '$dt', 'unit': group_type}},
                'sum_val': {'$sum': '$value'}}
            },
            {'$sort': {'_id': 1}},
            {'$group': {
                '_id': None,
                'dataset': {'$push': '$sum_val'},
                'labels': {'$push': {'$dateToString': {'format': '%Y-%m-%dT%H:%M:%S', 'date': '$_id'}}}
            }},
            {'$project': {'_id': 0}}
    ]

    
    result_data = list(series_collection.aggregate(request))

    current_date = dt_from
    date_list = []

    while current_date <= dt_upto:
        date_list.append(current_date)
        if group_type == "hour":
            current_date += timedelta(hours=1)
        elif group_type == "day":
            current_date += timedelta(days=1)
        elif group_type == "month":
            _, last_day = calendar.monthrange(current_date.year, current_date.month)
            current_date = current_date.replace(day=last_day) + timedelta(days=1)


    aggregated_data = []
    new_date = []
    for date in date_list:
        new_date.append(datetime.strftime(date, '%Y-%m-%dT%H:%M:%S'))
        if date.strftime('%Y-%m-%dT%H:%M:%S') in result_data[0]['labels']:
            index = result_data[0]['labels'].index(date.strftime('%Y-%m-%dT%H:%M:%S'))
            aggregated_data.append(result_data[0]['dataset'][index])
        else:
            aggregated_data.append(0)
    

    return {'dataset': aggregated_data, 'labels': new_date}

