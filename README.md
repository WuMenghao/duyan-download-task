#### 1、介绍

本项目是对通用文件导出任务的抽象组件提取，通过使用本项目，可以简化通用文件导相关业务的开发。

#### 2、使用方式

示例：

1、配置文件

```ini
[download_task]
dbHost=localhost
dbPort=3306
dbUser=root
dbPassword=12345678
dbName=cti
redisHost=localhost
redisPort=6379
redisDb=0
```

2、任务分发调度及状态管理

```python
from duyan_download_task import DownloadTaskScheduler


def test_scheduler():
    scheduler = DownloadTaskScheduler('config.ini', 'download_task_scheduler')
    scheduler.start()


if __name__ == '__main__':
    test_scheduler()

```

3、自定义导出业务逻辑

```python
import json

from duyan_download_task import DownloadTaskBase, SubTypeInfo
from duyan_download_task.model import DownloadTaskMain, DownloadItem


class DemoTask(DownloadTaskBase):

    def __init__(self, task_info: SubTypeInfo, config_path: str, logger_name: str):
        super().__init__(task_info, config_path, logger_name)

    def get_validated_data_params(self, task_item_id: int, data: str) -> dict or None:
        self._logger.info(f"校验数据:[ID:{task_item_id}],data:{data}")
        return json.loads(data)

    def export(self, task_item: DownloadItem, data_obj: dict) -> tuple:
        self._logger.info(f"校验导出:[item:{data_obj}],data:{data_obj}")
        return None, None, None

    def split_task(self, task: DownloadTaskMain) -> list:
        pass


def task_test():
    info = SubTypeInfo(task_name='DemoTask', type=10, sub_type=15, queue_key='demo_task_queue', is_multi_task=False)
    task = DemoTask(info, 'config.ini', 'demo_task')
    task.start()


if __name__ == '__main__':
    task_test()

```