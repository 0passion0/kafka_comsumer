from fasttransform import Transform


class TranlateDatePipeline(Transform):

    def apply(self, value):
        value.data['info_date'] = value.data['info_date'][:7]
        return value

    def encodes(self, obj):
        for i in range(len(obj)):
            obj[i] = self.apply(obj[i])
        return obj


