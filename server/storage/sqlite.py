import sqlite3
from .base import BaseStorage

import logging

logger = logging.getLogger(__name__)


class SQLiteStorage(BaseStorage):
    def connect(self) -> bool:

        if self._connection:
            logger.warning(f'Already connected to SQLite database {self.database}')
            return True

        try:
            self._connection = sqlite3.connect(self.database)
            logger.info(f'Connected to SQLite database {self.database}, version {sqlite3.version}')
            return True
        except sqlite3.Error as e:
            logger.error(f'Error connecting to SQLite database {self.database}: {e}')

            if self._connection:
                self.disconnect()

            return False

    def disconnect(self) -> bool:
        try:
            self._connection.close()
            logger.info(f'Disconnected from SQLite database {self.database}')
            return True
        except sqlite3.Error as e:
            logger.error(f'Error disconnecting from SQLite database {self.database}: {e}')
            return False

    def execute(self, query: str) -> [bool, list]:
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f'Error executing query: {e}')
            return False
