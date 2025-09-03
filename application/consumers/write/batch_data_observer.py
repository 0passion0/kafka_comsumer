import pathway as pw
import time as tt
class BatchDataObserver(pw.io.python.ConnectorObserver):
    """
    批量数据观察者类，用于批量处理和输出数据
    """
    def __init__(self, batch_size=20, time_window=60):
        super().__init__()
        self.batch = []
        self.batch_size = batch_size
        self.time_window = time_window
        self.last_batch_time = tt.time()

    def on_change(self, key, row, time: int, is_addition: bool):
        """
        当数据发生变化时调用
        """
        self.batch.append(row)
        now = tt.time()
        if len(self.batch) >= self.batch_size or (now - self.last_batch_time >= self.time_window):
            self._flush(now)

    def on_close(self):
        """
        当连接关闭时调用
        """
        if self.batch:
            self._flush(tt.time())

    def _flush(self, now):
        """
        刷新缓冲区并输出数据
        """
        print(f"\n[BatchObserver] 批量处理 {len(self.batch)} 行数据:")
        print("=" * 50)
        print(self.batch[0])
        # for r in self.batch:
        #     print(r)
        self.batch = []
        self.last_batch_time = now

