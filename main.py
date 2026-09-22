"""FastAPI 服务入口：暴露本地异步任务队列。

数据库与队列参数均通过 JOBQUEUE_* 环境变量配置（见 README）。
"""

from jobqueue.api import create_app
from jobqueue.config import Settings

app = create_app(Settings.from_env())
