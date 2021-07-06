FROM python:3.8.0
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
CMD ["./services/publish_message_pubsub.py"]
ENTRYPOINT ["python3"]