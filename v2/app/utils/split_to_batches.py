def to_batches(urls: list, batch_length: int) -> list:
    from math import floor
    batches = floor(len(urls) / batch_length) + 1
    result = list()
    if batches <= 1:
        return [urls]
    for batch in range(batches - 1):
        result.append(urls[batch_length * batch:batch_length * (batch + 1)])
    if len(urls[batch_length * (batches - 1):]) > 0:
        result.append(urls[batch_length * (batches - 1):])
    return result