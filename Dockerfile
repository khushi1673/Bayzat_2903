FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Install python dependencies
RUN pip install boto3 psycopg2-binary

COPY etl_tool.py .

# Environment variables (to be overridden by docker-compose)
ENV SQS_ENDPOINT=http://localstack:4566
ENV SQS_QUEUE_NAME=test-queue
ENV DB_HOST=postgres
ENV DB_NAME=sqs_data
ENV DB_USER=user
ENV DB_PASSWORD=password
ENV AWS_DEFAULT_REGION=ap-south-1
ENV AWS_ACCESS_KEY_ID=test
ENV AWS_SECRET_ACCESS_KEY=test

CMD ["python", "etl_tool.py"]
