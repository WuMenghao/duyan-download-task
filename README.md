#### 1、介绍

本项目是对通用文件导出任务的抽象组件提取，通过使用本项目，可以简化通用文件导相关业务的开发。

#### 2、使用方式

数据库表:
```sql
CREATE TABLE `download_task_main` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `org_id` bigint unsigned NOT NULL,
  `task_name` varchar(64) DEFAULT NULL COMMENT '任务名称',
  `platform` int NOT NULL COMMENT ' 生成平台 ， 1 ：cfg , 2:cti , 3 : open , 4: crm ',
  `type` int NOT NULL COMMENT '导出类型 1: 导出联系人 ， 2 ：公司对账单导出 3 ： 导出 计划item 4 ：导出 通话记录统计分析 5 ： 录音下载 6 ：信修下载 7 .通话记录加录音下载',
  `sub_type` int NOT NULL COMMENT '导出子类型 导出类型下 分出来的子类型 ',
  `description` varchar(255) DEFAULT NULL COMMENT '描述',
  `status` int DEFAULT '0' COMMENT ' 0 :任务创建中 , 1 : 文件上传中 ，2 ：文件上传完成 ,-1 :文件上传失败 3 : 任务失效  4：取消任务',
  `top` int DEFAULT '0' COMMENT '是否置顶   0：否  1：是',
  `data` varchar(1024) DEFAULT NULL COMMENT '序列化字段',
  `notify` tinyint DEFAULT '0' COMMENT '是否通知 0：否 1：是',
  `remark` varchar(256) DEFAULT NULL COMMENT '备注',
  `created_by` bigint DEFAULT '0' COMMENT '创建人 , agent org id ',
  `created_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `index_created_time` (`created_time`),
  KEY `index_org_id_created_time` (`org_id`,`created_time`),
  KEY `index_type` (`type`),
  KEY `index_type_org_id` (`type`,`org_id`),
  KEY `index_org_id_platform` (`org_id`,`platform`),
  KEY `index_task_name_org_id` (`task_name`,`org_id`),
  KEY `index_sub_type_status` (`sub_type`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='下载任务表';

CREATE TABLE `download_item` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键 ',
  `task_id` bigint NOT NULL COMMENT '主任务id',
  `org_id` bigint unsigned NOT NULL COMMENT ' 公司ID ',
  `type` int DEFAULT NULL COMMENT '下载类型。一般跟 task的 子类型相同 ',
  `file_name` varchar(256) DEFAULT NULL COMMENT '文件名称',
  `target_id` bigint DEFAULT NULL COMMENT '关联id',
  `expire_time` timestamp NULL DEFAULT NULL COMMENT '过期时间',
  `download_times` int DEFAULT NULL COMMENT '下载次数',
  `url` varchar(1024) DEFAULT NULL COMMENT '文件下载地址',
  `status` int DEFAULT '0' COMMENT '0 :未开始 ， 1: 进行中 ，2:已完成 ，3 ：失效(针对下载次数超限) -1 :任务失败  4：取消任务',
  `data` varchar(1024) DEFAULT NULL COMMENT '序列化字段',
  `remark` varchar(256) DEFAULT NULL COMMENT '备注',
  `md5_value` varchar(45) DEFAULT NULL COMMENT '文件MD5',
  `created_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `index_created_time` (`created_time`),
  KEY `index_task_id` (`task_id`),
  KEY `index_task_id_org_id` (`task_id`,`org_id`),
  KEY `index_org_id_target_id` (`org_id`,`target_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='下载任务明细';
```

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

3、自定义导出业务逻辑-单个子任务

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

4、自定义导出业务逻辑-多个子任务

```python
import json
import datetime

from duyan_download_task import DownloadTaskBase, SubTypeInfo
from duyan_download_task.model import DownloadTaskMain, DownloadItem
from duyan_download_task.constants import ItemStatus

class DemoMultiItemTask(DownloadTaskBase):
    task_info = SubTypeInfo(task_name='DemoMultiItemTask', type=11, sub_type=11, queue_key='demo_multi_item_task_queue',
                            is_multi_task=True)

    def __init__(self, config_path: str, logger_name: str):
        super().__init__(self.task_info, config_path, logger_name)

    def get_validated_data_params(self, task_item_id: int, data: str) -> dict or None:
        self._logger.info(f"校验数据:[ID:{task_item_id}],data:{data}")
        return json.loads(data)

    def export(self, task_item: DownloadItem, data_obj: dict) -> tuple:
        self._logger.info(f"校验导出:[item:{data_obj}],data:{data_obj}")
        url = 'http://duyan-record-download.oss-cn-hangzhou.aliyuncs.com/100757218773722020-04-27.zip?OSSAccessKeyId=LTAI4GFAeToJVw9E8eoLK8Wu&Expires=1968908476&Signature=KugYlSKBX6d6ExiDWcJKk4KoA1Y%3D'
        md5 = 'f9d991b4a0522dfa77da67d68c675261'
        file_name = '100757218773722020-04-27.zip'
        return url, md5, file_name

    def split_task(self, task: DownloadTaskMain) -> list:
        items = []
        now = datetime.datetime.now()
        for i in range(2):
            item = DownloadItem()
            item.org_id = task.org_id
            item.task_id = task.id
            item.type = self.task_info.sub_type
            item.data = task.data
            item.status = ItemStatus.READY.value
            item.created_time = now
            item.last_updated_time = now
            items.append(item)
        return items

def multi_item_task_test():
    task = DemoMultiItemTask('config.ini', 'demo_task')
    task.start()


if __name__ == '__main__':
    multi_item_task_test()
```