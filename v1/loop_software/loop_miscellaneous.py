from datetime import datetime


SUCCESS_RESPONSES = [200, 201, 202, 203, 204]


def get_difference(first: list, second: list):
    in_first = set(first) - set(second)
    in_second = set(second) - set(first)
    return {"in_first": list(in_first), "in_second": list(in_second)}


def split_into_batches(batch_length, urls: list) -> list:
    from math import floor
    batches = floor(len(urls) / batch_length) + 1
    result = list()
    if batches <= 0:
        return [urls]
    for batch in range(batches - 1):
        result.append(urls[batch_length * batch:batch_length * (batch + 1)])
    if len(urls[batch_length * (batches - 1):]) > 0:
        result.append(urls[batch_length * (batches - 1):])
    return result


def format_time(date: datetime = datetime.now()) -> str:
    return date.strftime("%Y%m%d")
