#!/usr/bin/env python

from base_statistics import *

count_succ = 1
count_all = 3
count_succ = get_count_succ()
count_all = get_count_all()

value = round(float(count_succ) / float(count_all), 4)
update_kpi("dispatch_succ_rate", value)
update_kpi("dispatch_count_succ", int(count_succ))
update_kpi("dispatch_count_all", int(count_all))
