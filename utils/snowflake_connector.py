import snowflake.connector

def create_snowflake_ctx(ctx_detail):
    user = ctx_detail["user"]
    account = ctx_detail["account"]
    role = ctx_detail["role"]

    ctx = snowflake.connector.connect(
        user = user,
        account = account,
        role = role,
        authenticator = "externalbrowser",
    )
    
    return ctx

def submit_sql(ctx, sql):
    
    cs = ctx.cursor()
    try:
        cs.execute(sql)
        one_row = cs.fetchone()
        
        return one_row[0] 
    finally:
        cs.close()