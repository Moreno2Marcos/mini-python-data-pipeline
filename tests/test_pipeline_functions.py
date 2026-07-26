import unittest

from src.transform import transform_users
from src.validate import validate_users


class TestPipelineFunctions(unittest.TestCase):

    def setUp(self):
        self.users = [
            {
                "id": 1,
                "name": "Usuário Teste",
                "email": "teste@email.com",
                "address": {
                    "city": "Recife",
                    "zipcode": "50000-000",
                    "geo": {
                        "lat": "-8.0000",
                        "lng": "-34.0000",
                    },
                },
                "company": {
                    "name": "Empresa Teste",
                },
            }
        ]

        self.processed_at = "2026-07-15T10:00:00"

    def test_validate_users_returns_true(self):
        result = validate_users(self.users)

        self.assertTrue(result)

    def test_transform_users_returns_expected_data(self):
        result = transform_users(
            self.users,
            self.processed_at,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["user_id"], 1)
        self.assertEqual(result[0]["city"], "Recife")
        self.assertEqual(result[0]["company_name"], "Empresa Teste")
        self.assertEqual(
            result[0]["processed_at"],
            self.processed_at,
        )


if __name__ == "__main__":
    unittest.main()
