
import commands

def exec_commands(cmd):
    status, output = commands.getstatusoutput(cmd)
    if status == 0:
        return output

    print cmd
    print status
    print output
    raise Exception("exec_commands error: " + cmd)


def hive_query(sql):
    cmd = "hive -S -e '" + sql + "'"
    return exec_commands(cmd)


def mysql_query(sql):
    cmd = "mysql -h127.0.0.1 -udispatch -pdispatch -e \"" + sql + "\""
    return exec_commands(cmd)


def update_kpi(key, value, datetime = 0):
    sql = """
    replace into dispatch.kpi (\`key\`, \`value\`, \`datetime\`)
    values ('%s', '%.4f', '%s')
    """ %(key, value, datetime)
    mysql_query(sql)
