FROM python:3.11-slim

WORKDIR /app

# 创建必要的目录
RUN mkdir -p /app/logs /app/config

# 复制项目文件
COPY requirements.txt .
COPY app/ ./app/
COPY main.py .

# 设置pip源为阿里云源
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ && \
    pip config set install.trusted-host mirrors.aliyun.com

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 设置环境变量
ENV PYTHONPATH=/app
ENV CONFIG_PATH=/app/config
ENV LOG_PATH=/app/logs

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["python", "main.py"]
