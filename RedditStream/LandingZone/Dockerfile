# official Python runtime as a parent image
FROM python:3.9-slim

# working directory in the container
WORKDIR /app

# xopy the requirements file + install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy code into the container
COPY main.py .

# expose port 8080 for Cloud Run
EXPOSE 8080

# run
CMD ["python", "main.py"]
