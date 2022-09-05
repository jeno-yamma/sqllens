"""
Check for Micropartition
"""
import json

from utils import plan_parser, snowflake_connector


def get_execution_plan(ctx, sql):
    sql = f"explain using json {sql}"

    try:
        execution_plan = snowflake_connector.submit_sql(ctx, sql)
        execution_plan = json.loads(execution_plan)
        
        return execution_plan
    except:
        return False
    

def check_micropartition_over_threshold(total_partition, partition_scanned, micropartition_read_theshold):
    
    if (partition_scanned/total_partition)*100 > micropartition_read_theshold:
        return True
    else:
        return False

def check_table_has_cluster_key(ctx, table_full_definition):
    sql = f"select system$clustering_information('{table_full_definition}')"

    try:
        clustering_information = snowflake_connector.submit_sql(ctx, sql)
        clustering_information_j = json.loads(clustering_information)
        cluster_key = clustering_information_j["cluster_by_keys"]
        
        return True, cluster_key
    except:
        return False, ""
                
def evaluate_pruning(sql, ctx_detail):
    ctx = snowflake_connector.create_snowflake_ctx(ctx_detail)

    plan = get_execution_plan(ctx, sql)

    tables = {}
    attribute = plan_parser.get_filter_used(plan)

    for k, v in attribute.items():

        total_partition, partition_scanned, bytes_assigned = plan_parser.get_global_statistic(plan)

        is_over = check_micropartition_over_threshold(total_partition, partition_scanned, 20)

        has_ck, ck = check_table_has_cluster_key(ctx, k)
        ck = ck.strip("LINEAR()").split(",")

        if len(v) == 0 and is_over: 
            i_look_efficient = {"i_look_efficient": False}
            tablescan_note = {"tablescan_is_high": True} 
            v = {"pruning_column_used": False} 

            if has_ck: 
                table_pruning_column = {"table_pruning_column": ck}            
            else: 
                table_pruning_column = {"table_pruning_column": ""}
            
            tables[k] = {**i_look_efficient, **tablescan_note, **v, **table_pruning_column}

        elif len(v) > 0 and is_over:
            
            i_look_efficient = {"i_look_efficient": False}
            tablescan_note = {"tablescan_is_high": True} 

            for column in v:                
                
                if column.lower() in ck:
                    v = {"pruning_column_used": True} 
                else: 
                    table_pruning_column = {"table_pruning_column": ck}
                    v = {"pruning_column_used": False} 
            
            tables[k] = {**i_look_efficient, **tablescan_note, **v, **table_pruning_column}

        else:
            i_look_efficient = {"i_look_efficient": True}
            tablescan_note = {"tablescan_is_high": False} 
            v = {"pruning_column_used": False} 

            if has_ck:
                table_pruning_column = {"table_pruning_column": ck}
            else: 
                table_pruning_column = {"table_pruning_column": []}

            tables[k] = {**i_look_efficient, **tablescan_note, **v, **table_pruning_column}

    return(tables)
