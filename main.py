"""
Тестовое задание.
Для воспроизведения случая описанного в ТЗ, и отсутствующего в БД:
- если для какого-то обращения нет ни одной предшествующей сессии, его
(обращение) все равно нужно вернуть с пустой информацией о сессии.

Сформирован фейковый датасет, с проверкой результатов запроса:
http://sqlfiddle.com/#!15/65abc/2
"""

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


# Формируем строку соединения с БД.
connection_string = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
    os.environ.get('DB_USER'),
    os.environ.get('DB_PASS'),
    os.environ.get('DB_HOST'),
    os.environ.get('DB_PORT'),
    os.environ.get('DB_NAME'),
)

# Подключаемся к БД.
engine = create_engine(connection_string)

SQL_QUERY = """
WITH t as (
    SELECT 
          comm.communication_id
        , comm.site_id
        , comm.visitor_id
        , comm.communication_date_time
        , sess.visitor_session_id
        , sess.session_date_time
        , sess.campaign_id
        , CASE WHEN comm.communication_date_time < sess.session_date_time 
          THEN NULL ELSE comm.communication_date_time - sess.session_date_time END as delta
        , sess.row_n

    FROM
        (SELECT 
              site_id
            , visitor_id
            , visitor_session_id
            , date_time as session_date_time
            , campaign_id
            , ROW_NUMBER() OVER (
                PARTITION BY site_id, visitor_id
                ORDER BY date_time ASC
            ) as row_n

        FROM sessions

        WHERE 
            site_id in (SELECT site_id FROM communications) 
            AND visitor_id in (SELECT visitor_id FROM communications)
            )  as sess
        FULL JOIN  
        (SELECT 
              communication_id
            , site_id
            , visitor_id
            , date_time as communication_date_time
            , date_time as session_date_time
        FROM communications
        ) as comm
        ON comm.site_id=sess.site_id and comm.visitor_id=sess.visitor_id
)


SELECT 
      SUB.communication_id
    , SUB.site_id
    , SUB.visitor_id
    , SUB.communication_date_time
    , t.visitor_session_id
    , t.session_date_time
    , t.campaign_id
    , t.row_n
FROM
    (SELECT 
          communication_id
        , site_id
        , visitor_id
        , communication_date_time
        , min(delta) as delta
    FROM t
    GROUP BY site_id, visitor_id, communication_id, communication_date_time
    ) SUB
LEFT JOIN t ON
SUB.communication_id=t.communication_id AND SUB.delta=t.delta
ORDER BY site_id, visitor_id, communication_date_time
"""


if __name__ == '__main__':
    df = pd.read_sql_query(SQL_QUERY, engine)
    df.to_csv('result_{0}.csv'.format(os.environ.get('DB_USER')))