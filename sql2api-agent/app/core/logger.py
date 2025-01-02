import logging
import sys
from logging.handlers import RotatingFileHandler
import os

# 从环境变量获取日志路径
LOG_PATH = os.getenv('LOG_PATH', 'logs')

# 确保日志目录存在
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

# 创建logger
logger = logging.getLogger('sql2api')
logger.setLevel(logging.DEBUG)

# 日志格式
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 文件处理器 - 使用环境变量中的路径
file_handler = RotatingFileHandler(
    os.path.join(LOG_PATH, 'sql2api.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# 控制台处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# 添加处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler) 