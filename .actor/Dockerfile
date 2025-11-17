FROM apify/actor-python:3.13
COPY . ./
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
CMD ["python", ".actor/src/main.py"]
