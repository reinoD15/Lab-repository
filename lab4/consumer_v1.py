from confluent_kafka import Consumer
import string

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book_processor',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)
topic = 'book_topic'
consumer.subscribe([topic])

stop_words = {"the", "and", "of", "to", "a", "in", "that", "is", "was", "he", "for", "it", "with", "as", "his", "on", "be", "at", "by", "i", "this", "had", "not", "are", "but", "from", "or", "who"}

MAX_EMPTY_POLLS = 20
empty_polls = 0

print("En attente de messages...")

# Ouverture du fichier de sortie en mode ajout ("a")
with open("cleaned_book.txt", "a", encoding="utf-8") as out_file:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Fermeture : Plus de messages depuis 20 secondes.")
                break
            continue
            
        if msg.error():
            print(f"Erreur : {msg.error()}")
            break

        empty_polls = 0
        
        # 1. Décodage du message brut
        raw_line = msg.value().decode('utf-8')
        
        # 2. Nettoyage (Pipeline NLP)
        words = raw_line.split(' ')
        clean_words = []
        for word in words:
            # Minuscule et retrait de la ponctuation
            w = word.lower().strip(string.punctuation + ' «»\n\r')
            if w != '' and w not in stop_words:
                clean_words.append(w)
                
        # 3. Écriture dans le fichier si la ligne n'est pas vide après nettoyage
        if clean_words:
            clean_line = " ".join(clean_words)
            out_file.write(clean_line + "\n")
            print(f"Ligne traitée : {clean_line}")

consumer.close()
