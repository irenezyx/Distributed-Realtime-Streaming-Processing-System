if __name__ == "__main__":
    import json
    dict = {"spout": ["filter"], "filter": ["transform"], "transform": []}
    dict = json.loads(json.dumps(dict))
    for key in dict.keys():
        print(key)
    print(list(dict.keys()))
    d = {'a': 0, 'b': 1, 'c': 2}
    l = d.keys()

    print(l)
