import abc

import loguru
import redis

from src.model import DownloadItem
from src.utils import common_utils, logger_util, mysql_util
from src.utils.config_util import Config


class ExportTaskBase:

    def __init__(self, task_type: int, job_queue: dict, config_path: str, logger_name: str, max_job=1,
                 cron="0 0-59/2 * * * *"):
        self._logger = logger_util.LOG.get_logger(logger_name, enqueue=True)
        # 配置
        self._config = Config(config_path)
        # mysql配置
        self.db = mysql_util.db(self._config.db_config.host,
                                self._config.db_config.port,
                                self._config.db_config.user,
                                self._config.db_config.password,
                                self._config.db_config.db_name)
        # redis链接池
        self._redisPool = redis.ConnectionPool(host=self._config.redis_config.host,
                                               port=self._config.redis_config.port,
                                               password=self._config.redis_config.password,
                                               db=self._config.redis_config.db)
        # redis客户端
        self.redisCli = redis.Redis(connection_pool=self._redisPool)
        self.TASK_TYPE = task_type
        self.TASK_QUEUE_KEY = job_queue
        # cron表达式和并发配置
        self.CRON = common_utils.get_cron_trigger_for_str(cron)
        self.MAX_JOb = max_job
        self._logger.info("程序开始...")

    def init(self):
        items = self.get_running()
        self._logger.info(f"开始处理上次未完成的任务，共{items and len(items) or 0}条")
        if items is None or len(items) == 0:
            return
        for item in items:
            self.execute_unit_job(item.get("id"))

    def get_running(self):
        sql = f"select * from download_item where type = {self.TASK_TYPE} and status = 1 and created_time between '{(datetime.now() - timedelta(days=1)).strftime(datetime_util.DEFAULT_DATETIME_FORMAT)}' and '{datetime.now().strftime(datetime_util.DEFAULT_DATETIME_FORMAT)}'"
        return self.db.select(sql)

    def get_task(self):
        """
        获取任务
        :return:
        """
        result = self.redisCli.lpop(self.TASK_QUEUE_KEY)
        if result:
            if isinstance(result, bytes):
                result = bytes.decode(result, encoding="utf-8")
            task_item_id = int(result)
        else:
            task_item_id = None
        return task_item_id

    def get_task_batch(self, batch_size: int) -> list:
        """
        批量获取任务
        :param batch_size:
        :return:
        """
        result = []
        pipeline = self.redisCli.pipeline()
        for i in range(0, batch_size - 1):
            pipeline.lpop(self.TASK_QUEUE_KEY)
        response = pipeline.execute()
        for one in response:
            if one is None:
                continue
            result.append(int(bytes.decode(one, encoding="utf-8")))
        return result

    def finish_task_with_error(self, item_id, data=None, message="未知错误"):
        """
        失败更新
        :param item_id:
        :param data:
        :param message:
        :return:
        """
        if data is None:
            data = json.dumps({'error': message}, ensure_ascii=False)
        else:
            if common_utils.is_json(data):
                data = json.loads(data, encoding="utf-8")
                data['error'] = message
                data = json.dumps(data, ensure_ascii=False)
            else:
                data += message
        try:
            self.db.execute(DOWNLOAD_ITEM_ERROR_UPDATE, {'data': data, 'id': item_id})
        except Exception as e:
            self._logger.exception(e)
            raise Exception(e)

    def update_success_item(self, item_id, url, md5, file_name):
        """
        成功更新
        :param item_id:
        :param url:
        :return:
        """
        try:
            invalid_time = common_utils.get_date_around_today(days=+3, origin_date=datetime.now())
            self.db.execute(DOWNLOAD_ITEM_FINISH_UPDATE,
                            {'url': url, 'id': item_id, 'md5': md5, 'invalidTime': invalid_time, 'fileName': file_name})
            self._logger.info(f"[ID:{item_id}]任务状态已完成, url:{url}, md5:{md5}")
        except Exception as e:
            self._logger.exception(e)
            raise Exception(e)

    def update_processing_item(self, item_id):
        """
        进行中更新
        :param item_id:
        :return:
        """
        try:
            self.db.execute(DOWNLOAD_ITEM_STATE_UPDATE, {'id': item_id})
            self._logger.info(f"[ID:{item_id}]任务状态正在进行中")
        except Exception as e:
            self._logger.exception(e)
            raise Exception(e)

    def get_task_item(self, item_id):
        """
        获取任务数据
        :param item_id:
        :return:
        """
        items = self.db.select_by_param(DOWNLOAD_ITEM_QUERY, {'id': item_id})
        if len(items) > 0:
            self._logger.info(f"[ID:{item_id}]任务获取成功,item:{item_id}")
            return items[0]
        return None

    @loguru.logger.catch
    def execute_job(self):
        """
        逻辑执行入口
        :return:
        """
        self.heart_beat()
        task_item_id = self.get_task()
        self.execute_unit_job(task_item_id)

    @loguru.logger.catch
    def execute_batch_job(self, batch_size: int = 10):
        """
        批量任务执行入口
        :param batch_size:
        :return:
        """
        assert batch_size > 0
        task_item_ids = self.get_task_batch(batch_size)
        for task_item_id in task_item_ids:
            self.execute_unit_job(task_item_id)

    def execute_unit_job(self, task_item_id: int) -> object:
        """
        执行任务但愿
        :param task_item_id:
        :return:
        """
        data = None
        task_id = None
        item_id = None
        try:
            if task_item_id is None:
                return
            self._logger.info(f"ITEM:[ID:{task_item_id}],任务开始执行")

            # 查询任务记录
            task_item_obj = self.get_task_item(task_item_id)
            if task_item_obj is None:
                self._logger.info(f"ITEM:[ID:{task_item_id}],任务不存在,数据库无法找到对应记录")
                return
            self._logger.info(f"ITEM:[ID:{task_item_id}],任务开始执行")

            # 判断data不为空 参数校验
            task_item = DownloadItem(task_item_obj)
            task_id = task_item.task_id
            item_id = task_item.id
            data = task_item.data
            if task_item.data is None or str(task_item.data).strip() == '':
                self._logger.warning(f"TASK[ID:{task_id}],ITEM[ID:{item_id}],任务执行失败,data为空")
                self.finish_task_with_error(task_item_id, data, message="任务执行失败,data为空")
                return

            if not common_utils.is_json(data):
                self._logger.warning(f"TASK[ID:{task_id}],ITEM[ID:{item_id}],任务执行失败,data格式异常")
                self.finish_task_with_error(task_item_id, data, message="任务执行失败,data格式异常")
                return

            if task_item.org_id is None:
                self._logger.warning(f"TASK[ID:{task_id}],ITEM[ID:{item_id}],任务执行失败,orgId为空")
                self.finish_task_with_error(task_item_id, data, message="任务执行失败,orgId为空")
                return

            data_obj = self.get_validated_data_params(task_item_id, data)
            if data_obj is None:
                return

            status = task_item.status
            if status > 1:
                self._logger.warning(f"TASK[ID:{task_id}],ITEM[ID:{item_id}],任务状态为：{status}，不予处理.")
                return

            # 更新为下载中
            self.update_processing_item(task_item_id)
            # 导出上传逻辑
            url, md5_value, file_name = self.export(task_item, data_obj)
            # 跟新为已完成
            self.update_success_item(task_item_id, url, md5_value, file_name)
        except Exception as e:
            self._logger.error(f"TASK[ID:{task_id}],ITEM[ID:{item_id}],任务执行失败,error:{e}")
            self._logger.exception(e)
            self.finish_task_with_error(task_item_id, data, f"任务执行失败;{e}")
            # raise e

    @abc.abstractmethod
    def get_validated_data_params(self, task_item_id: int, data: str) -> dict or None:
        pass

    @abc.abstractmethod
    def export(self, task_item: DownloadItem, data_obj: dict) -> tuple:
        pass
