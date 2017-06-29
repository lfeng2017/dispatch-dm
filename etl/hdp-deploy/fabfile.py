#coding:utf-8

from fabric.api import local,cd,put,lcd,env,run,sudo
from fabric.contrib.files import append, sed
from fabric.contrib.console import confirm
from fabric.context_managers import *

env.hosts = [
    'lujin@172.17.2.48:22',
    'lujin@172.17.2.60:22',
    'lujin@172.17.2.61:22',
    'lujin@172.17.0.45:22',
    'lujin@172.17.0.46:22',
    'lujin@172.17.0.47:22',
    'lujin@172.17.0.48:22',
]
env.password='123Qwe,./'

# 设置无密码登录
def setPswless():
    with lcd('/home/lujin/'):
        put('~/.ssh/id_rsa.pub','/home/lujin/')
    with cd('/home/lujin/'):
        run('cat  ~/id_rsa.pub  >> /home/lujin/.ssh/authorized_keys')

# 安装java，并修改环境变量
def install_java():
    with lcd('/home/lujin/'):
        put('jdk-8u121-linux-x64.tar.gz','/home/lujin/')
    with cd('/home/lujin/'):
        sudo('rm -fr /opt/jdk-8u121-linux-x64')
        sudo('tar -zxvf jdk-8u121-linux-x64.tar.gz -C /opt/', warn_only=True, quiet=True)
        append('/etc/profile', '# jdk setting', use_sudo=True)
        append('/etc/profile', 'export JAVA_HOME=/opt/jdk1.8.0_121', use_sudo=True)
        append('/etc/profile', 'export CLASSPATH=.:$JAVA_HOME/lib:$JAVA_HOME/jre/lib:$CLASSPATH', use_sudo=True)
        append('/etc/profile', 'export PATH=$JAVA_HOME/bin:$JAVA_HOME/jre/bin:$PATH', use_sudo=True)
        run('source /etc/profile')
        run('java -version')

# 升级python27
def install_python27():
    with lcd('/home/lujin/'):
        put('Python-2.7.13.tgz','/home/lujin/')
    with cd('/home/lujin/'):
        run('rm -fr Python-2.7.13')
        run('tar zxf Python-2.7.13.tgz', warn_only=True, quiet=True)
    with cd('/home/lujin/Python-2.7.13'):
        run('./configure', warn_only=True, quiet=True)
        run('make', warn_only=True, quiet=True)
        sudo('make install', warn_only=True, quiet=True)
        sudo('mv /usr/bin/python /usr/bin/python2.6.6')
        sudo('ln -s /usr/local/bin/python2.7 /usr/bin/python')
        append('/etc/rc.local', 'ln -s /usr/local/bin/python2.7 /usr/bin/python', use_sudo=True)

# 挂载本地hdp yum源
def setLocalRepo4HDP():
    with lcd('/home/lujin/'):
        put('~/yongche-hdp.repo','/home/lujin/')
    with cd('/home/lujin/'):
        sudo('mv  /home/lujin/yongche-hdp.repo  /etc/yum.repos.d/')
        run('yum repolist | grep HDP')

# 安装ambari agent （若ambari-server自动安装失败，则使用该方式）
def install_ambari_agent():
    with settings(prompts={'Is this ok [y/N]: ': 'y'}):
        sudo('yum install ambari-agent', warn_only=True, quiet=True)
    # 修改配置
    conf_path = "/etc/ambari-agent/conf/ambari-agent.ini"
    ambari_server = "gut1.epcs.bj2.yongche.com"
    sed(conf_path, "hostname=localhost", "hostname={}".format(ambari_server), use_sudo=True)
    sudo('ambari-agent start')

# 关闭THP（ambari建议）
def disableTHP():
    sudo('echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag')
    sudo('echo never > /sys/kernel/mm/redhat_transparent_hugepage/enabled')
    sudo('echo never > /sys/kernel/mm/transparent_hugepage/enabled')
    sudo('echo never > /sys/kernel/mm/transparent_hugepage/defrag')


def clearAmbari():
    sudo('rm -fr /home/y/hdp/*')
    #sudo('rm -fr /usr/hdp')
    sudo('rm -fr /var/hadoop')
    sudo('rm -fr /var/tmp/*')
#     sudo("ls -l /var/log/ | grep  -E 'hadoop|hive|zeppelin|falcon|hive' | awk '{print $9}' | sudo xargs rm -fr")
#     sudo("ls -l /var/run/ | grep  -E 'hadoop|hive|zeppelin|falcon|hive' | awk '{print $9}' | sudo xargs rm -fr")
    sudo("find /var/log/ -depth -group hadoop -exec rm -rf {} \;")
    sudo("find /var/run/ -depth -group hadoop -exec rm -rf {} \;")
    sudo("find /var/log/ -depth -group hive -exec rm -rf {} \;")
    sudo("find /var/run/ -depth -group hive -exec rm -rf {} \;")
    sudo("find /var/log/ -depth -group hbase -exec rm -rf {} \;")
    sudo("find /var/run/ -depth -group hbase -exec rm -rf {} \;")
    sudo("find /var/log/ -depth -group falcon -exec rm -rf {} \;")
    sudo("find /var/run/ -depth -group falcon -exec rm -rf {} \;")
    sudo("rm -fr /var/home/y/hdp")
    sudo("rm -fr /var/var/hdp-link")


# 磁盘软连接（ambari不允许安装到/home开通目录）
def mappingHDFS():
    sudo('mkdir -p /home/y/hdp/')
    sudo('ln -s /home/y/hdp /var/hdp-link')
    append('/etc/rc.local', 'ln -s /home/y/hdp /var/hdp-link', use_sudo=True)
