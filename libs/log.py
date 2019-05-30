from libs.env import Env
import logging
import os
import sys


class Log():
    """封装了一下Logger"""
    log_file_name = 'running.log'
    env = Env()
    if not os.path.exists(env.log_path):
        os.mkdir(env.log_path)

    file_logger = logging.FileHandler(os.path.join(env.log_path, log_file_name), 'a')
    logger = logging.getLogger('performance')
    # 设置日志格式
    formatter = logging.Formatter(fmt='%(asctime)s[%(filename)s line:%(lineno)d][%(levelname)s] %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    file_logger.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_logger)
    logger.addHandler(logging.StreamHandler(sys.stdout))


class AttachedLog():

    @staticmethod
    def get_logger(device, app, ts):
        log_file_name = '{an}-{av}-{dev}-{t}-running.log'.format(an=app.app_name, av=app.version_code, dev=device.model, t=ts)
        env = Env()
        if not os.path.exists(env.log_path):
            os.mkdir(env.log_path)

        file_logger = logging.FileHandler(os.path.join(env.log_path, log_file_name), 'w+')
        logger = logging.getLogger('performance')
        # 设置日志格式
        formatter = logging.Formatter(fmt='%(asctime)s[%(filename)s line:%(lineno)d][%(levelname)s] %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        file_logger.setFormatter(formatter)
        logger.setLevel(logging.INFO)
        logger.addHandler(file_logger)
        logger.addHandler(logging.StreamHandler(sys.stdout))

        return logger


if __name__ == '__main__':
    Log.logger.info("test")
