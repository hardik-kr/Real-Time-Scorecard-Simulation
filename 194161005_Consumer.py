import threading
import os
import shutil
from kafka import KafkaConsumer
import score

bootstrap_servers = ['localhost:9092'] 

if os.path.isdir("Kafka_Consumer"):
    shutil.rmtree("Kafka_Consumer")
    os.mkdir("Kafka_Consumer")
else:
    os.mkdir("Kafka_Consumer")

def consumer1():
    topicName = '4143' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0

    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))
               

def consumer2():
    topicName = '4144' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer3():
    topicName = '4145' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer4():
    topicName = '4146' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer5():
    topicName = '4147' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer6():
    topicName = '4148' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer7():
    topicName = '4149' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer8():
    topicName = '4150' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer9():
    topicName = '4151' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer10():
    topicName = '4152' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer11():
    topicName = '4153' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer12():
    topicName = '4154' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer13():
    topicName = '4155' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer14():
    topicName = '4157' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer15():
    topicName = '4158' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer16():
    topicName = '4159' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer17():
    topicName = '4160' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer18():
    topicName = '4161' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer19():
    topicName = '4162' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer20():
    topicName = '4163' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer21():
    topicName = '4165' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer22():
    topicName = '4166' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer23():
    topicName = '4168' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer24():
    topicName = '4169' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer25():
    topicName = '4170' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer26():
    topicName = '4171' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer27():
    topicName = '4172' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer28():
    topicName = '4173' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer29():
    topicName = '4174' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer30():
    topicName = '4175' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer31():
    topicName = '4176' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer32():
    topicName = '4177' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer33():
    topicName = '4178' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer34():
    topicName = '4179' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer35():
    topicName = '4180' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer36():
    topicName = '4182' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer37():
    topicName = '4183' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer38():
    topicName = '4184' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer39():
    topicName = '4186' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer40():
    topicName = '4187' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer41():
    topicName = '4188' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer42():
    topicName = '4190' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer43():
    topicName = '4191' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
def consumer44():
    topicName = '4192' 
    consumer = KafkaConsumer(topicName,bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest')
    d=os.getcwd()
    d1=os.path.join(d,'Kafka_Consumer')
    inp = 'Kafka_Consumer'+topicName+'_Commentary_1st.txt'
    inp2 = 'Kafka_Consumer'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
    fname1=os.path.join(d1,inp2)
 
    l1 = 0
    l2 = 0
 
    for message in consumer:
        key = message.key.decode('utf-8')
        if key=="1st":
            file = open(fname,'a')
            commentary = message.value.decode('utf-8')
            file.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary)) 
            file.close()
            l1+=1
            if l1>=5:
                score.scorecard(int(topicName))

        elif key=="2nd":
            file1 = open(fname1,'a')
            commentary = message.value.decode('utf-8')
            file1.write(commentary)
            print("Match ID : %-2s Innings : %-2s Commentary :- %s" %(message.topic,key,commentary))
            file1.close()
            l2+=1
            if l2>=5:
                score.scorecard(int(topicName))      
    
    


t1 = threading.Thread(target=consumer1)
t2 = threading.Thread(target=consumer2)
t3 = threading.Thread(target=consumer3)
t4 = threading.Thread(target=consumer4)
t5 = threading.Thread(target=consumer5)
t6 = threading.Thread(target=consumer6)
t7 = threading.Thread(target=consumer7)
t8 = threading.Thread(target=consumer8)
t9 = threading.Thread(target=consumer9)
t10 = threading.Thread(target=consumer10)
t11 = threading.Thread(target=consumer11)
t12 = threading.Thread(target=consumer12)
t13 = threading.Thread(target=consumer13)
t14 = threading.Thread(target=consumer14)
t15 = threading.Thread(target=consumer15)
t16 = threading.Thread(target=consumer16)
t17 = threading.Thread(target=consumer17)
t18 = threading.Thread(target=consumer18)
t19 = threading.Thread(target=consumer19)
t20 = threading.Thread(target=consumer20)
t21 = threading.Thread(target=consumer21)
t22 = threading.Thread(target=consumer22)
t23 = threading.Thread(target=consumer23)
t24 = threading.Thread(target=consumer24)
t25 = threading.Thread(target=consumer25)
t26 = threading.Thread(target=consumer26)
t27 = threading.Thread(target=consumer27)
t28 = threading.Thread(target=consumer28)
t29 = threading.Thread(target=consumer29)
t30 = threading.Thread(target=consumer30)
t31 = threading.Thread(target=consumer31)
t32 = threading.Thread(target=consumer32)
t33 = threading.Thread(target=consumer33)
t34 = threading.Thread(target=consumer34)
t35 = threading.Thread(target=consumer35)
t36 = threading.Thread(target=consumer36)
t37 = threading.Thread(target=consumer37)
t38 = threading.Thread(target=consumer38)
t39 = threading.Thread(target=consumer39)
t40 = threading.Thread(target=consumer40)
t41 = threading.Thread(target=consumer41)
t42 = threading.Thread(target=consumer42)
t43 = threading.Thread(target=consumer43)
t44 = threading.Thread(target=consumer44)

t1.start()
t2.start()
t3.start()
t4.start()
t5.start()
t6.start()
t7.start()
t8.start()
t9.start()
t10.start()
t11.start()
t12.start()
t13.start()
t14.start()
t15.start()
t16.start()
t17.start()
t18.start()
t19.start()
t20.start()
t21.start()
t22.start()
t23.start()
t24.start()
t25.start()
t26.start()
t27.start()
t28.start()
t29.start()
t30.start()
t31.start()
t32.start()
t33.start()
t34.start()
t35.start()
t36.start()
t37.start()
t38.start()
t39.start()
t40.start()
t41.start()
t42.start()
t43.start()
t44.start()
