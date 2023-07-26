from airflow.hooks.mysql_hook import MySqlHook
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# MySQL config
mysqlhook = MySqlHook(mysql_conn_id = 'MySQLWB')

engine_kwargs = {'connect_args': {'charset': 'utf8mb4'}}
Session = sessionmaker(bind=mysqlhook.get_sqlalchemy_engine(engine_kwargs))

session = Session()


def upsert(row, table):
    title = row['title']
    article_id = row['article_id']
    article_url = row['article_url']
    year = row['year']
    month = row['month']

    record = session.query(table).filter_by(title=title, article_id=article_id, article_url=article_url, year=year, month=month).first()
    
    if not record:
        record = table()

    record.title = title
    record.article_id = article_id
    record.article_url = article_url
    record.year = year
    record.month = month

    session.merge(record)
    session.commit()


# def create_table(tablename):
#     connection = mysqlhook.get_conn()
#     mysqlhook.set_autocommit(connection, True)
#     cursor = connection.cursor()

#     sql = """CREATE TABLE IF NOT EXISTS `{}` (
#           `title` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
#           `article_id` int COLLATE utf8mb4_unicode_ci DEFAULT NULL,
#           `article_url` varchar(4096) COLLATE utf8mb4_unicode_ci NOT NULL,
#           `year` int COLLATE utf8mb4_unicode_ci NOT NULL,
#           `month` int COLLATE utf8mb4_unicode_ci NOT NULL,
#           PRIMARY KEY (`id`)
#         )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""".format(tablename)

#     cursor.execute(sql)
#     cursor.close()