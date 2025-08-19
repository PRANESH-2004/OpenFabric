from locust import HttpUser, task, between

class TransactionUser(HttpUser):
    wait_time = between(1, 3)  # seconds

    @task
    def submit_transaction(self):
        self.client.post(
            "/api/transactions",
            json={
                "amount": 10.5,
                "currency": "USD",
                "description": "Load test transaction"
            }
        )
