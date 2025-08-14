import copy
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from fasttransform import Transform, Pipeline

from application.utils.decorators import log_execution


@dataclass
class Data:
    title: str
    author: str
    viewCount: int


class StripTitle(Transform):
    def encodes(self, x: Data): x.title = x.title.strip(); return x


class NormalizeAuthor(Transform):
    out_fields = ['author']

    def encodes(self, x: Data):
        time.sleep(3)
        x.author = x.author.title()
        return x


class ClampViewCount(Transform):
    out_fields = ['viewCount']

    def encodes(self, x: Data):
        time.sleep(3)
        x.viewCount = max(0, int(x.viewCount))
        return x

class ParallelGroup(Transform):
    def __init__(self, *transforms):
        super().__init__()
        self.transforms = transforms

    def encodes(self, x):
        with ThreadPoolExecutor(max_workers=len(self.transforms)) as ex:
            futures = {ex.submit(t, copy.deepcopy(x)): t for t in self.transforms}

            for fut in as_completed(futures):
                res = fut.result()
                t = futures[fut]
                #: 如果 transform 有指定 out_fields，则只更新这些字段
                out_fields = getattr(t, 'out_fields', None)
                if out_fields:
                    for f in out_fields:
                        setattr(x, f, getattr(res, f))
                else:
                    #: 没指定则更新全部
                    x.__dict__.update(res.__dict__)
        return x


pipeline = Pipeline([
    StripTitle(),
    ParallelGroup(NormalizeAuthor(), ClampViewCount()),  # 并行
])