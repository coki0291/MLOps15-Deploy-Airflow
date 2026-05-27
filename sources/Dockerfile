FROM python:3.8-slim

ENV PYTHONUNBUFFERED True

ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /base

RUN pip install --upgrade pip
RUN pip install scikit-learn
RUN pip install pandas==1.5.3
RUN pip install numpy
