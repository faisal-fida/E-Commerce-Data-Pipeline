#!/bin/bash
# Run database setup
python src/setup_db.py
# Start the consumer
exec python src/consumer.py