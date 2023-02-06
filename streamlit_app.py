%%writefile app.py

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import json 
from json import loads
from kafka import KafkaConsumer

kafka_topic_name = "vsstyjzs-fishai"
kafka_bootstrap_servers = 'dory-01.srvs.cloudkafka.com:9094'

user="vsstyjzs"
password="s1-tllo707MBX3bbVMXT-c-EcNqH3jSW"


consumer=KafkaConsumer(
    kafka_topic_name,
    bootstrap_servers=kafka_bootstrap_servers,
    auto_offset_reset='earliest',
    # enable_auto_commit=True,
    # group_id="my_group",\
    # client_id="my_client",
    security_protocol="SASL_SSL",\
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=user,\
    sasl_plain_password =password,\
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
import pandas as pd
df=pd.DataFrame(columns=['order_id','TimeStamp','Gross_weight'])
try:
  count=0
  for message in consumer:
    feed=message.value
    df.loc[count,:]=feed.split(",")
    df.TimeStamp=pd.to_datetime(df.TimeStamp)
    count+=1
  print(f"Receive {count} message")
except KeyboardInterrupt:
  print(f"Receive {count} message")

st.write("Hello")
st.line_chart(df)
