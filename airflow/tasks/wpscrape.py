from airflow.hooks.mysql_hook import MySqlHook
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# MySQL config
mysqlhook = MySqlHook(mysql_conn_id = 'MySQLWB')

engine_kwargs = {'connect_args': {'charset': 'utf8mb4'}}
Session = sessionmaker(bind=mysqlhook.get_sqlalchemy_engine(engine_kwargs))

session = Session()


def upsert(row, table):
    title = row["title"]["rendered"]
    date = row["date"]
    link = row["link"]

    record = session.query(table).filter_by(title=title, date=date, link=link).first()
    
    if not record:
        record = table()

    record.title = title
    record.date = date
    record.link = link

    session.merge(record)
    session.commit()