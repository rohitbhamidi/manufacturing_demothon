FROM python:3.12-bookworm
WORKDIR /manufacturing_demothon
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY . .

CMD ["python", "manufacturing_demothon/manufacturing.py"]
