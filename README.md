buat folder 
<img width="364" height="403" alt="image" src="https://github.com/user-attachments/assets/a3caa76c-6be6-407a-b7be-c85c8308f93d" />

urutan menjalakan kafka buka cmd di directory kafka
CMD 1 
1. Jalankan kafka broker
- Buat uuid : 
bin\windows\kafka-storage.bat format -t kafka-storage-random-uuid -c config\server.properties
atau jika uuid gak keluar, pakai
bin\windows\kafka-storage.bat random-uuid

- Format storage dengan UUID
bin\windows\kafka-storage.bat format -t <UUID-yang-tadi> -c config\server.properties

-Menjalkan brokernya (CMD jangan di tutup)
bin\windows\kafka-storage.bat format --standalone -t aONxBJzaREuvUYftU8U5-Q -c config\server.properties

-jika sudah pernah menjalankan kafka cukup :
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties

BUKA CMD 2(cmd baru) di directory kafka
2. BUAT Topic
- transaction
.\bin\windows\kafka-topics.bat --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
- transaction-dlq
.\bin\windows\kafka-topics.bat --create --topic transactions-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
-Cek list topic
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

BUKA CMD 3 (cmd BARU) 
3. Menjalankan Consumer 
bin\windows\kafka-console-consumer.bat --topic transactions --from-beginning --bootstrap-server localhost:9092

Buka CMD 4 (CMD BARU)
4. Menjalankan Producer
bin\windows\kafka-console-producer.bat --topic transactions --bootstrap-server localhost:9092

---------------Di VSCODE---------------------------
terminal 1
python -m src.consumer
ketika berjalan, jangan dimatikan, setelah itu jalankan python producer. 

kalau udah muncul semua output baik di consumer dan producer, ctr + c untuk menghentikan program consumer. Ketika dihentikan nanti dia muncul quality_summary.csv

terminal 2
 python -m src.producer
