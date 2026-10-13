import json
import logging
from dataclasses import dataclass, field
from typing import Optional

import boto3
import psycopg2
from psycopg2 import pool as pg_pool

from .config import REGION, db_port

logger = logging.getLogger(__name__)


@dataclass
class DatabaseManager:
    _pool: Optional[object] = field(default=None, init=False, repr=False)

    def _get_secret(self):
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=REGION)
        response = client.get_secret_value(SecretId="DocumentCompareV3")
        return json.loads(response['SecretString'])

    def get_pool(self):
        if self._pool is None or getattr(self._pool, "closed", True):
            secret = self._get_secret()
            self._pool = pg_pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=2,
                user=secret['username'],
                password=secret['password'],
                host=secret['host'],
                port=db_port,
                database=secret['engine']
            )
        return self._pool

    def execute_query(self, query, params=None, fetch=False, return_last_id=False):
        pool = self.get_pool()
        for attempt in range(2):
            conn = pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute(query, params)
                result = None
                if fetch:
                    result = cursor.fetchall()
                if return_last_id:
                    cursor.execute("SELECT lastval();")
                    result = cursor.fetchone()[0]
                conn.commit()
                cursor.close()
                pool.putconn(conn)
                return result
            except psycopg2.OperationalError as e:
                print("DB connection lost, retrying...", str(e))
                try:
                    pool.putconn(conn, close=True)
                except Exception:
                    pass
                if attempt == 1:
                    raise
            except Exception as e:
                conn.rollback()
                pool.putconn(conn)
                print("DB query failed:", str(e))
                raise

    def update(self, query, params):
        try:
            self.execute_query(query, params)
            return 200
        except Exception:
            return 500

    def insert(self, query, values):
        return self.execute_query(query, values, return_last_id=True)

    def fetch(self, query, params=None):
        return self.execute_query(query, params, fetch=True)

    def update_time(self, message):
        query = """
        UPDATE document_compare_results
        SET time_report =
            COALESCE(time_report::jsonb, '{}'::jsonb)
            || jsonb_build_object(%s, NOW() AT TIME ZONE 'Asia/Kolkata')
        WHERE conversation_id = %s;
        """
        self.update(query, message)

    def try_initiate_extraction(self, unique_id):
        """Atomically claim the extraction trigger once both S3 paths are present. Returns True only once."""
        pool = self.get_pool()
        conn = pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE document_compare_results
                   SET status = 'extracting'
                   WHERE conversation_id = %s
                     AND digital_doc IS NOT NULL AND digital_doc <> ''
                     AND handscanned_doc IS NOT NULL AND handscanned_doc <> ''
                     AND status NOT IN ('initiated', 'completed', 'failed')
                   RETURNING conversation_id""",
                (unique_id,)
            )
            claimed = cursor.fetchone() is not None
            conn.commit()
            cursor.close()
            return claimed
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)

    def try_initiate_compare(self, unique_id):
        """Atomically claim the compare trigger — returns True only for the first caller."""
        pool = self.get_pool()
        conn = pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE document_compare_results
                   SET status = 'initiated'
                   WHERE conversation_id = %s
                     AND hand_scanned_results IS NOT NULL
                     AND digital_scanned_results IS NOT NULL
                     AND status NOT IN ('initiated', 'completed', 'failed')
                   RETURNING conversation_id""",
                (unique_id,)
            )
            claimed = cursor.fetchone() is not None
            conn.commit()
            cursor.close()
            return claimed
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)
