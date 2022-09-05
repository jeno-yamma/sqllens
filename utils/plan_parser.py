import re
from typing import List, Tuple

from data.execution_plan_result import *

def get_global_statistic(plan):
    global_stats = plan['GlobalStats']

    total_partition = global_stats['partitionsTotal']
    partition_scanned = global_stats['partitionsAssigned']
    bytes_assigned = global_stats['bytesAssigned']

    return total_partition, partition_scanned, bytes_assigned

def get_column_in_scanned(plan):
    column_dict = {}

    for ops in plan['Operations'][0]:
        if ops.get('expressions') and ops.get('operation') == 'TableScan':
            columns_raw = [re.search(r'[^\.]*$', column_raw).group(0) for column_raw in ops.get('expressions')]

            column_dict[ops['id']] = columns_raw
    
    return column_dict

def get_column_in_result(plan):
    column_dict = {}

    for ops in plan['Operations'][0]:
        if ops.get('expressions') and ops.get('operation') == 'Result':
            columns_raw = [re.search(r'[^\.]*$', column_raw).group(0) for column_raw in ops.get('expressions')]

            column_dict[ops['id']] = columns_raw
    
    return column_dict

def get_filter_used(plan): 
    hold = {}

    for ops in plan['Operations'][0]:  

        if ops.get("objects") and ops.get("operation") == "TableScan":
            table_name = ops.get("objects")[0]
            hold[table_name] = []

        if ops.get('expressions') and ops.get('operation') == 'Filter': 
            table_name = plan['Operations'][0][ops.get("id") + 1].get("objects")[0]

            columns = [] 
            
            column_list_raw = ops.get('expressions')[0].split('AND')
            for column in column_list_raw:
                field = re.findall('\.([^\s]+)', column)
                if field is not None:
                    columns.append(field[0])

            hold[table_name] = columns

    return hold