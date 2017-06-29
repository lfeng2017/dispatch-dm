
from tools import *

def get_count_succ():
    hql_count_succ = """
    select count(distinct service_order_id)
    from ods.dispatch_info
    where bidding_id > 0
    and decision_driver_id > 0
    """
    return hive_query(hql_count_succ)


def get_count_all():
    hql_count_total = """
    select count(distinct service_order_id)
    from ods.dispatch_info
    where bidding_id > 0
    """
    return hive_query(hql_count_total)

