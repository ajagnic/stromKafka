import unittest
from Strom.strom.database.mongo_management import MongoManager
from Strom.strom.dstream.dstream import DStream


class TestMongoManager(unittest.TestCase):
    def setUp(self):
        self.manager = MongoManager()
        self.dstream = DStream()
        self.dstream["device_id"] = 'Chad'

    def test_insert(self):
        inserted_id = self.manager._insert(self.dstream, 'template')
        queried = self.manager._get_by_id(inserted_id, 'template')

        inserted_id2 = self.manager._insert(self.dstream, 'derived')
        queried2 = self.manager._get_by_id(inserted_id2, 'derived')

        inserted_id3 = self.manager._insert(self.dstream, 'event')
        queried3 = self.manager._get_by_id(inserted_id3, 'event')

        self.assertEqual("Chad", queried["device_id"])
        self.assertEqual("Chad", queried2["device_id"])
        self.assertEqual("Chad", queried3["device_id"])

        self.assertEqual(inserted_id, queried["_id"])
        self.assertEqual(inserted_id2, queried2["_id"])
        self.assertEqual(inserted_id3, queried3["_id"])


if __name__ == "__main__":
    unittest.main()