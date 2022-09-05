from lens.performance import micropartition_scan
from secret.ctx_detail import CTX_DETAIL

fd = open('query.sql', 'r')
sql = fd.read()
fd.close()

outcome = micropartition_scan.evaluate_pruning(sql, CTX_DETAIL)

print(outcome)