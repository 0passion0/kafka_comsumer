from application.pipelines.base_pipeline import BasePipeline


class TranlateDatePipeline(BasePipeline):

    def apply(self, value):
        value.data['info_date'] = value.data['info_date'][:7]  # 简单字符串截取
        return value
