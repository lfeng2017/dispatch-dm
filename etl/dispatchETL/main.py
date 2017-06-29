# -*- coding:utf-8 -*-
import os.path as op
from cement.core.foundation import CementApp
from cement.core.controller import CementBaseController, expose
from cement.core.exc import FrameworkError, CaughtSignal
# from cement.ext.ext_colorlog import ColorLogHandler
from cement.utils.misc import init_defaults
from cli.BdpLoader import BdpHiveController
from cli.FlumeParser import KafkaHiveController
from cli.MysqlLoader import MysqlController

VERSION = '0.0.1'
APP_NAME = "DispatchETL"
CONF_NAME = "app.conf"

BANNER = """
DispatchETL Application v%s
Copyright (c) 2017 yongche.com
""" % VERSION

# for colorlog
COLORS = {
    'DEBUG': 'cyan',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'red,bg_white',
}

# 主方法的绝对路径
BASE_DIR = op.dirname(op.abspath(__file__))

# 设置默认的配置项（代码层级）
defaults = init_defaults(APP_NAME, 'log.logging')
defaults['log.logging']['file'] = op.abspath(op.join(BASE_DIR, "../logs/" + APP_NAME + ".log"))


def set_default_env(app):
    # 若未指定ENV参数，则从配置文件中加载默认的ENV参数
    if app.pargs.env is None:
        app.pargs.env = app.config.get("default", "default_env")
        app.log.warning("unspecitied Environment, use [default_env] in {}".format(CONF_NAME))
    app.log.info('Current Environment: {}'.format(app.pargs.env))

    # 根据ENV参数，读取对应的配置文件，组成cfg配置字典（后续配置项改从cfg获取）
    app.cfg = dict(
        app.config.get_section_dict('env.%s' % app.pargs.env).items()
        + app.config.get_section_dict('default').items()
        + app.config.get_section_dict(APP_NAME).items()
    )

    # debug模式，打印配置项信息
    if app.cfg.get('debug', False):
        for k, v in app.cfg.items():
            app.log.info("CONFIG {k} => {v}".format(k=k, v=v))

class AppBaseController(CementBaseController):
    """
    程序入口的默认Controller，显示基本信息
    """

    class Meta:
        label = 'base'
        description = "welcome to use DistpachETL"
        arguments = [
            (['-v', '--version'], dict(action='version', version=BANNER)),
        ]

    @expose(hide=True)
    def default(self):
        print BANNER
        print("请使用 -h 命令查看模块，选择您需要的模块/命令进行执行")
        # shell("df -h")
        # self.app.log.info(" log test after module")


class MyApp(CementApp):
    class Meta:
        label = APP_NAME
        base_controller = 'base'
        handlers = [
            AppBaseController,
            BdpHiveController,
            KafkaHiveController,
            MysqlController,
        ]
        config_defaults = defaults
        config_files = [
            op.join(BASE_DIR, CONF_NAME)
        ]
        arguments_override_config = True
        # log_handler = ColorLogHandler(colors=COLORS)
        exit_on_close = True


def main():
    with MyApp() as app:
        try:
            app.hook.register('post_argument_parsing', set_default_env)
            app.args.add_argument('-E', '--environment', help='指定运行的环境', action='store', nargs='?',
                                  choices=['prod', 'dev'], dest='env')
            app.run()
        except CaughtSignal as e:
            from signal import SIGINT, SIGABRT

            if e.signum == SIGINT:
                app.exit_code = 110
            elif e.signum == SIGABRT:
                app.exit_code = 111

        except FrameworkError as e:
            print("FrameworkError => %s" % e)

            app.exit_code = 300

        finally:
            if app.debug:
                import traceback
                traceback.print_exc()


if __name__ == '__main__':
    main()
