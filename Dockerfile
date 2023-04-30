FROM apache/beam_python3.7_sdk:2.25.0
WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./pipeline.py" ]