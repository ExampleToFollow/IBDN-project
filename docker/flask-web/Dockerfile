FROM python:3.8-slim

WORKDIR /app

COPY resources/web/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY resources/web /app/resources/web

EXPOSE 5001

CMD ["python", "/app/resources/web/predict_flask_project.py"]