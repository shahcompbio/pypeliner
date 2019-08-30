import jsonpickle


def load(picklefile):
    data = picklefile.readlines()

    assert len(data) == 1, data

    jsonpickle.decode(data[0], keys=True)

    return jsonpickle.decode(data[0], keys=True)


def dump(data, picklefile):
    data = jsonpickle.encode(data, keys=True)

    data = data.encode()

    picklefile.write(data)
