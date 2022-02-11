FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
WORKDIR ~/academy-capstone
COPY ./requirements.txt .
USER root
RUN pip install -r ./requirements.txt
#USER 185
COPY ./task_1 ./task_1
CMD ["python3",  "./task_1/step_1.py"]