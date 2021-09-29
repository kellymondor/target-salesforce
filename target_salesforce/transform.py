def map_to_salesforce_fields(mapping, line):
    
    if mapping.get(line["stream"]):
        stream_mapping = mapping.get(line["stream"])
        fields = stream_mapping.get("fields", None)
        salesforce_object = stream_mapping.get("salesforce_object")    
        record = { fields.get(k, k): v for k, v in line["record"].items() if k in list(fields.keys())}
    else:
        raise Exception(f"Mapping for stream {stream_name} not found in config.")

    return salesforce_object, record