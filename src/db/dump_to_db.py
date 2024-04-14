import subprocess
from src.config import DATABASE_NAME, DATABASE_COLLECTION


def dump_db():
    subprocess.run(['mongorestore', '--db', DATABASE_NAME, '--drop', '--collection',
                    DATABASE_COLLECTION, 'dump/sampleDB/sample_collection.bson'])


if __name__ == '__main__':
    dump_db()
