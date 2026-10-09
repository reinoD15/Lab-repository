import socket
import time
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)
topic = 'book_topic'

print("Début de l'envoi du livre...")

# Lecture du fichier et envoi ligne par ligne
with open("book.txt", "r", encoding="utf-8") as file:
    for line in file:
        # On ignore les lignes totalement vides
        if line.strip(): 
            producer.produce(topic=topic, value=line.encode('utf-8'))
            # Léger délai pour simuler un flux continu sans surcharger la mémoire
            time.sleep(0.01)

producer.flush()
print("Envoi terminé.")
