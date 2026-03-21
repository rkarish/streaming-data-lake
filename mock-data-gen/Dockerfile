FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY mock_data_gen/ mock_data_gen/

RUN pip install --no-cache-dir .

CMD ["python", "-m", "mock_data_gen.generator"]
