# Start by using python3 image
FROM python:3
WORKDIR /usr/src/app

# Install required python packages
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Set the image to execute python script on startup
COPY spark_control.py ./
CMD ["python", "-u", "./spark_control.py"]