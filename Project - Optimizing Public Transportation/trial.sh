kafka-topics --list --zookeeper localhost:2181
cd producers
python connector.py
cd ../
kafka-console-consumer --bootstrap-server localhost:9092 --topic postgre_connect_stations --from-beginning
cd consumers
faust -A faust_stream worker -l info

cd ../
kafka-console-consumer --bootstrap-server localhost:9092 --topic org.chicago.cta.weather.v1 --from-beginning

cd producers
python simulation.py

cd consumers
python server.py


# delete topics
kafka-topics --zookeeper localhost:2181 --delete --topic arrival.station.*
kafka-topics --zookeeper localhost:2181 --delete --topic turnstile_station
kafka-topics --zookeeper localhost:2181 --delete --topic org.chicago.cta.weather.v1