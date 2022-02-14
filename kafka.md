#Start kafka and hadoop services \
#activate venvspark \
#Create topic: \
(venvspark) [train@10 data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic office-input --replication-factor 1 --partitions 3
\

#cd data-generator \
#Kafka producer code: \
(venvspark) [train@10 data-generator]$ python dataframe_to_kafka.py --input /home/train/atscale4/final_homework/test_df/test_df.csv  -t office-input --excluded_cols 'pir_value' --sep ','
\

#Kafka consumer code : \
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic office-input
\

#Streaming kısmı için 2 topic oluşturalım;\
(venvspark) [train@10 data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic office-activity --replication-factor 1 --partitions 3
(venvspark) [train@10 data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic office-no-activity --replication-factor 1 --partitions 3
\

#topic lere yazılmış mı kontrol edelim
kafka-console-consumer.sh --bootstrap-server localhost:9092 --c office-activity
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic office-no-activity
