**************************************Python programming Assingment 5**********************************
**********************************************README***************************************************

Prerequisite to run Correctly:
1) Download and Install Kafka
2) Donwload the python module for Kafka Producer and consumer from Python PIP
3) Run the Zookeeper Server after successfull running zookeeper server now run the kafka server

Running the program
1) Now Firstly run the "producer.py". It will produce all commmentary in random manner into the Kafka server to the respective topic
2) Just after running Producer code we can run "consumer.py" it will consume all the commnetaries which the producer has consumed and also the consumer will generate the score card in the real time
3) To check generated scorecard in real time open the computed scorecard file in the folder "Kafka_Consumer_Computed_scorecard" in any of the editor you will see that scorecard is updating in a real time.


Note:- 1) As all the commmentary of all the matches is producing in the real time and also the scorecard is computing after every commmentary
       2) So therefore the RUNNING time COMPLEXITY of this program is very high
       3) Whole Program will take a running time of approximately 45 min to 60 min. 


