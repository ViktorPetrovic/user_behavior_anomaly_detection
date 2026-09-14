import logging
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pathlib import Path 
import uuid


logger = logging.getLogger(__name__)

class EventGenerator:
    def __init__(self, num_users: int = 50, seed: int = 42):
        self.num_users = num_users
        random.seed(seed)
        np.random.seed(seed)

        self.user_ids = [f"USER_{i:04d}" for i in range(num_users)]

        self.cities = ["Moscow", "London", "New York", "Tokyo", "Paris"]

        self.events = ['click', 'purchase', 'login', 'logout']

        self.devices = ['iOS', 'Android', 'Web']

        self.avg_purchase_amount = 500.0
        logger.info("Генератор создан с {num_users} пользователей")

    def generate_normal_event(self, user_id: str = None) -> dict:
        if user_id is None:
            user_id = random.choice(self.user_ids)

        event_type = random.choice(self.events)

        if event_type == 'purchase':
            amount = round(np.random.normal(self.avg_purchase_amount, 200), 2)
            amount = max(10.0, amount)

        return {
            'event_id': str(uuid.uuid4()),
            'user_id':user_id,
            'event_type': event_type,
            'timestamp': datetime.now().isoformat,
            'amount': amount,
            'city': random.choice(self.cities),
            'device': random.choice(self.devices),
            'is_anomaly': 0

            }

    def geberate_anomaly_event(self, user_id: str = None) -> dict:
        pass
    