# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import openpyxl
import pymysql

class DbPipeline:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', port=3306,
                                    user='root', password='PengYang@123',
                                    database='spider', charset='utf8mb4')
        self.cursor = self.conn.cursor()

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        title = item.get('title', '')
        rank = item.get('rank', 0)
        subject = item.get('subject', '')
        duration = item.get('duration', '')
        intro = item.get('intro', '')
        self.cursor.execute(
            'INSERT INTO tb_top_movie (`title`, `rating`, `subject`, `duration`, `intro`) VALUES(%s, %s, %s, %s, %s)',
            (title, rank, subject, duration, intro)
        )
        # 可以单条提交 也可以关闭连接的时候一次性提交
        # self.conn.commit()
        return item


class DbBatchPipeline:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', port=3306,
                                    user='root', password='PengYang@123',
                                    database='spider', charset='utf8mb4')
        self.cursor = self.conn.cursor()
        self.data = []

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        if len(self.data) > 0:
            self._write_to_db()
        self.conn.close()

    def process_item(self, item, spider):
        title = item.get('title', '')
        rank = item.get('rank', 0)
        subject = item.get('subject', '')
        duration = item.get('subject', '')
        intro = item.get('subject', '')
        self.data.append((title, rank, subject, duration, intro))
        if len(self.data) == 100:
            self._write_to_db()
            self.data.clear()
        return item

    def _write_to_db(self):
        self.cursor.executemany(
            'INSERT INTO tb_top_movie (`title`, `rating`, `subject`, `duration`, `intro`) VALUES(%s, %s, %s, %s, %s)',
            self.data
        )
        self.conn.commit()


class ExcelPipeline:

    def __init__(self):
        self.wb = openpyxl.Workbook()
        self.ws = self.wb.active
        self.ws.title = 'Top250'
        self.ws.append(('标题', '评分', '主题', '时长', '简介'))

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        self.wb.save('电影数据.xlsx')


    def process_item(self, item, spider):
        title = item.get('title', '')
        rank = item.get('rank') or ''
        subject = item.get('subject', '')
        duration = item.get('duration', '')
        intro = item.get('intro', '')
        self.ws.append(
            (title, rank, subject, duration, intro)
        )
        return item
