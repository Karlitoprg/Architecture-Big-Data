FROM python:3.8

RUN pip install seaborn pymongo matplotlib streamlit pyspark

COPY ./model /model

EXPOSE 8501

CMD ["streamlit", "run", "/model/monitor_streamlit.py"]
