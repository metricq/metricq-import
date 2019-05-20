def simple_metricq_to_dataheap_db(metricq_name: str):
    return metricq_name.replace('.', '_').replace('-', '_')
