#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import arrow
from fabric.api import *

# 登录用户和主机名：
env.user = 'lujin'
env.hosts = ['124.250.26.88']

TARGERT_SERV = "172.17.2.48"
INSTALL_DIR = "~/jobs/"
PKG = "DispatchETL.tar.gz"
PROJECT = "DispatchETL"

def zip():
    ' 定义一个pack任务 '
    local('rm -f azkaban.zip')
    # 仅打包kafka的任务
    local('zip azkaban.zip azkaban/KafkaDailyParse/*.job')
    print "[INFO] azkaban job allInOne 打包完成!"

def pack():
    ' 定义一个pack任务 '
    tar_files = ['dispatchETL/', 'azkaban/', 'dist/', 'requirements.txt', 'setup.py']
    local('rm -f {pkg}'.format(pkg=PKG))
    local('rm -rf ./dist/*')
    local('rm -rf ./dispatchETL/libs/*')
    local('tar -czvf {pkg} --exclude=*.tar.gz --exclude=*.pyc  --exclude=*.log  --exclude=*.zip  --exclude=*.job {files} '
          .format(pkg=PKG, files=' '.join(tar_files)))
    print "[INFO] 打包完成!", PKG


def deploy():
    ' 标准部署任务: 备份+替换 '

    server = "{user}@{serv}".format(user=env.user, serv=TARGERT_SERV)

    # step 1. 通过跳板机发送至目标服务器
    # 远程服务器的临时文件
    remote_tmp_tar = '~/{pkg}'.format(pkg=PKG)
    run('rm -f %s' % remote_tmp_tar)
    # 上传tar文件至跳板机
    put(PKG, remote_tmp_tar)
    # 跳板机传递至目标机器
    pkg = os.path.join(INSTALL_DIR, PKG)
    run('ssh -t -p 22 {server} "rm -f {pkg}"'.format(server=server, pkg=pkg))
    run('scp {tar} {server}:{install_dir}'.format(tar=remote_tmp_tar, server=server, install_dir=INSTALL_DIR))
    print "[INFO] 上传完成:", PKG

    # 备份老项目
    tag = arrow.now().format("YYYYMMDD")
    bak_tar_name = "{proj}_{tag}.tar.gz".format(proj=PROJECT, tag=tag)
    bak_tar = os.path.join(INSTALL_DIR, bak_tar_name)
    proj_folder = os.path.join(INSTALL_DIR, PROJECT)
    run('ssh -t -p 22 {server} "rm -f {old_tar}"'.format(server=server, old_tar=bak_tar))
    run('ssh -t -p 22 {server} "if [ -d {folder} ]; then rm -fr {folder}/dispatchETL/libs/*; fi"'.format(server=server, folder=proj_folder))
    run('ssh -t -p 22 {server} "if [ -d {folder} ]; then tar -zcvf {tar} {folder}; fi"'.format(server=server, tar=bak_tar, folder=proj_folder))
    print "[INFO] 备份完成:", bak_tar

    # 解压并替换老项目
    run('ssh -t -p 22 {server} "if [ ! -d {folder} ]; then mkdir {folder}; fi"'.format(server=server, folder=proj_folder))
    run('ssh -t -p 22 {server} "rm -fr {folder}/azkaban"'.format(server=server, folder=proj_folder))
    run('ssh -t -p 22 {server} "rm -fr {folder}/dispatchETL"'.format(server=server, folder=proj_folder))
    run('ssh -t -p 22 {server} "rm -f {folder}/requirements.txt"'.format(server=server, folder=proj_folder))
    run('ssh -t -p 22 {server} "tar -zxvf {pkg} -C {folder}/"'.format(server=server, pkg=pkg, folder=proj_folder))
    run('ssh -t -p 22 {server} "rm {pkg}"'.format(server=server, pkg=pkg))
    print "[INFO] 替换完成:", PROJECT

def zipDeps():
    # 安装依赖包
    server = "{user}@{serv}".format(user=env.user, serv=TARGERT_SERV)
    proj_folder = os.path.join(INSTALL_DIR, PROJECT)
    run('ssh -t -p 22 {server} "cd {folder} && python setup.py"'.format(server=server, folder=proj_folder))
    print "[INF0] 依赖包打包完成"
