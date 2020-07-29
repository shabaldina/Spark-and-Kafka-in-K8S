FROM python:3
WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY python_download_csv.py ./
CMD ["python", "-u", "./python_download_csv.py"]