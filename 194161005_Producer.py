import threading
try:
    from kafka import KafkaProducer
except:
    print("Please Install the Kafka module")
    exit(0)
import os

bootstrap_servers = ['localhost:9092'] 



def producer1():
    topicName = '4143' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer2():
    topicName = '4144' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer3():
    topicName = '4145' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer4():
    topicName = '4146' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer5():
    topicName = '4147' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer6():
    topicName = '4148' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer7():
    topicName = '4149' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer8():
    topicName = '4150' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer9():
    topicName = '4151' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer10():
    topicName = '4152' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer11():
    topicName = '4153' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer12():
    topicName = '4154' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer13():
    topicName = '4155' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer14():
    topicName = '4157' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer15():
    topicName = '4158' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer16():
    topicName = '4159' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer17():
    topicName = '4160' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer18():
    topicName = '4161' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer19():
    topicName = '4162' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer20():
    topicName = '4163' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer21():
    topicName = '4165' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer22():
    topicName = '4166' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer23():
    topicName = '4168' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer24():
    topicName = '4169' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer25():
    topicName = '4170' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer26():
    topicName = '4171' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer27():
    topicName = '4172' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer28():
    topicName = '4173' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer29():
    topicName = '4174' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer30():
    topicName = '4175' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer31():
    topicName = '4176' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer32():
    topicName = '4177' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer33():
    topicName = '4178' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer34():
    topicName = '4179' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer35():
    topicName = '4180' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer36():
    topicName = '4182' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer37():
    topicName = '4183' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer38():
    topicName = '4184' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer39():
    topicName = '4186' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer40():
    topicName = '4187' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer41():
    topicName = '4188' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer42():
    topicName = '4190' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer43():
    topicName = '4191' 
    try:
        producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)

    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    

def producer44():
    topicName = '4192' 
    try:
       producer =KafkaProducer(bootstrap_servers=bootstrap_servers)
    except:
        print("Please run the Kafka SERVER")
        exit(0)
    d=os.getcwd()
    d1=os.path.join(d,'Commentary')
    inp = '194161005_ODI_'+topicName+'_Commentary_1st.txt'
    inp2 = '194161005_ODI_'+topicName+'_Commentary_2nd.txt'

    fname=os.path.join(d1,inp)
     
    for line in reversed(list(open(fname,'r'))):
        
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'1st').get(timeout=30)
    

    fname=os.path.join(d1,inp2)
     
    for line in reversed(list(open(fname,'r'))):
        temp = str.encode(line)
        producer.send(topicName,b'%s' % temp,key=b'2nd').get(timeout=30)
    




t1 = threading.Thread(target=producer1)
t2 = threading.Thread(target=producer2)
t3 = threading.Thread(target=producer3)
t4 = threading.Thread(target=producer4)
t5 = threading.Thread(target=producer5)
t6 = threading.Thread(target=producer6)
t7 = threading.Thread(target=producer7)
t8 = threading.Thread(target=producer8)
t9 = threading.Thread(target=producer9)
t10 = threading.Thread(target=producer10)
t11 = threading.Thread(target=producer11)
t12 = threading.Thread(target=producer12)
t13 = threading.Thread(target=producer13)
t14 = threading.Thread(target=producer14)
t15 = threading.Thread(target=producer15)
t16 = threading.Thread(target=producer16)
t17 = threading.Thread(target=producer17)
t18 = threading.Thread(target=producer18)
t19 = threading.Thread(target=producer19)
t20 = threading.Thread(target=producer20)
t21 = threading.Thread(target=producer21)
t22 = threading.Thread(target=producer22)
t23 = threading.Thread(target=producer23)
t24 = threading.Thread(target=producer24)
t25 = threading.Thread(target=producer25)
t26 = threading.Thread(target=producer26)
t27 = threading.Thread(target=producer27)
t28 = threading.Thread(target=producer28)
t29 = threading.Thread(target=producer29)
t30 = threading.Thread(target=producer30)
t31 = threading.Thread(target=producer31)
t32 = threading.Thread(target=producer32)
t33 = threading.Thread(target=producer33)
t34 = threading.Thread(target=producer34)
t35 = threading.Thread(target=producer35)
t36 = threading.Thread(target=producer36)
t37 = threading.Thread(target=producer37)
t38 = threading.Thread(target=producer38)
t39 = threading.Thread(target=producer39)
t40 = threading.Thread(target=producer40)
t41 = threading.Thread(target=producer41)
t42 = threading.Thread(target=producer42)
t43 = threading.Thread(target=producer43)
t44 = threading.Thread(target=producer44)

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
#metadata = ack.get()
#print(metadata)
