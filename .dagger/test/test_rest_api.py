import unittest
import requests
import os

endpoint = os.environ.get("ENV_HTTP_ENDPOINT", "localhost")

class TestRestApi(unittest.TestCase):
    def test_connection(self):
        res = requests.get(f"{endpoint}")
        
        self.assertTrue(res.ok)
        self.assertDictEqual(res.json(), {'message': 'hello world!'})

    def test_ftp_anonymous(self):
        res = requests.get(f"{endpoint}/ftp/anonymous", {'offset': 0, 'limit': 99})
        data = res.json()

        self.assertEqual(len(data), 2)
        self.assertListEqual(data, 
            [
                {
                    'is_anon': True, 
                    'target': '192.0.0.1'
                },
                {
                    'is_anon': True,
                    'target': '192.0.0.2'
                }
            ]
        )

        res = requests.get(f"{endpoint}/ftp/anonymous", {'offset': 0, 'limit': 1000})
        self.assertEqual(res.status_code, 422)

if __name__ == '__main__':
    unittest.main()