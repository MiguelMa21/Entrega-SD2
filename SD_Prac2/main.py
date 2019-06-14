import pywren_ibm_cloud as pywren 
import json
import pika,os
import random
from click._compat import raw_input

global _numSlaves

def leader():
	# Declarations
	global _numSlaves
	petitionList = []
	usedSlavesList = []
		
	# This is the consume function, or callback
	def leaderFunction ( channel , method , properties , body ):
		global _numSlaves
		nonlocal petitionList
		nonlocal usedSlavesList
		body = body.decode()
		print ( "Leader: body of message received = " + body )
		if ( len ( usedSlavesList ) == _numSlaves ):	# If all slaves have published
			print ( "Leader: all slaves have published." )
			channel.basic_publish ( exchange = 'num_queue' , routing_key = '' , body = '0' )	# Tell them to stop
			channel.stop_consuming()
			
		elif ( body not in usedSlavesList ):	# If this slave has already published, its petition will be ignored
			print ( "Leader: this slave has not published." )
			petitionList.append ( body )	# Otherwise, we will add its petition to our list
			
			if ( len ( petitionList ) == _numSlaves - len ( usedSlavesList ) ):	# If all slaves (that haven't published yet) have made their petition
				print ( "Leader: petition list full, choosing slave." )
				randNum = random.randint( 0 , len ( petitionList ) - 1 )		# Choose a random slave
				print ( "Leader: " + petitionList [ randNum ] + " is going to publish." )
				channel.basic_publish ( exchange = 'num_queue' , routing_key = '' , body = petitionList [ randNum ] )	# Tell all slaves 
																														# which one is going to publish
				usedSlavesList.append( petitionList [ randNum ] )	# Add this slave to used slaves
				petitionList = []	# Restore petition list
		
	# Establish communication
	pw_config = json.loads ( os.environ.get ( 'PYWREN_CONFIG' , '' ) )
	url = pw_config [ 'rabbitmq' ] [ 'amqp_url' ]		
	params = pika.URLParameters ( url )
	connection = pika.BlockingConnection ( params ) 
	channel = connection.channel ( )
	channel.queue_delete( 'leader_queue' )	# Delete the queue used in past execution
	
	# Code
	channel.queue_declare ( queue = 'leader_queue' )	# Declare the queue where slave
														# will make a petition to publish
	channel.basic_consume ( leaderFunction , queue = 'leader_queue' , no_ack = True )	# Configure how consume will be done
	channel.start_consuming ( )
	
	# Close communication
	channel.close ( )
	connection.close ( )
	return None

def slave ( identifier ):
	# Declarations
	messageList = []
	print ( "Slave " + identifier + ": initializing..." )
	
	# This is the consume function, or callback
	def slaveFunction( channel , method , properties , body ):
		nonlocal messageList
		nonlocal identifier
		body = body.decode()
		print ( "Slave " + identifier + ": this is the body: " + body )
		if ( body == identifier ):
			num = random.randint ( 1 , 1000 )
			print ( "Slave " + identifier + ": I have to publish " + str ( num ) )
			channel.basic_publish ( exchange = 'num_queue' , routing_key = '' , body = str ( num ) )
		elif ( body == '0' ):
			print ( "Slave " + identifier + ": I have STOP." )
			channel.stop_consuming()
		elif ( "id" not in body ):
			print ( "Slave " + identifier + ": saving number " + body )
			messageList.append( body )
			print ( "Slave " + identifier + ": so far " + str ( messageList ) )
			channel.basic_publish ( exchange = '' , routing_key = 'leader_queue' , body = identifier )
	
	# Establishing communication
	pw_config = json.loads ( os.environ.get ( 'PYWREN_CONFIG' , '' ) )
	url = pw_config [ 'rabbitmq' ] [ 'amqp_url' ]
	params = pika.URLParameters ( url )
	connection = pika.BlockingConnection ( params )
	channel = connection.channel()
	channel.queue_delete( identifier )	# Delete queue from last execution
	
	# Code
	channel.exchange_declare ( exchange = 'num_queue', exchange_type = 'fanout' )
	channel.queue_declare ( queue = identifier )
	channel.queue_bind ( exchange = 'num_queue' , queue = identifier )
	channel.basic_publish ( exchange = '' , routing_key = 'leader_queue' , body = identifier )	# Initial petition
	channel.basic_consume ( slaveFunction , queue = identifier , no_ack = True )
	channel.start_consuming()
	
	# Close communication
	channel.close()
	connection.close()
	print ( messageList )
	return messageList
	
def main ( ):
	print ( "Write the number of slaves that you want: ")
	global _numSlaves
	_numSlaves = int ( raw_input ( ) )
	
	pywren_client = pywren.ibm_cf_executor ( rabbitmq_monitor = True )
	
	# Create leader function
	params2leader = []
	pywren_client.call_async ( leader , params2leader )
	
	# In this loop, the params2slaves to all slaves are created.
	# Each slave must receive its ID
	params2slaves = [ ]
	for i in range ( 0 , _numSlaves ):
		identifier = "id" + str ( i )					# All slaves have this ID format id + number. For instance: id5
		params2slaves.append ( identifier )
	
	# Create all slaves
	pywren_client.map ( slave , params2slaves )
	results_list = pywren_client.get_result()
	results_list.remove ( None )
	print ( "Results:" )
	for i in range ( 0 , len ( results_list ) ):
		print ( results_list [ i ] )
	
	# Check whether all slaves have the same results or not
	equals = True
	i = 0
	while ( equals and i < _numSlaves ):
		equals = ( results_list [ i ] == results_list [ 0 ] )
		i = i + 1
		
	if ( equals ):
		print ( "All lists are equal." )
	else:
		print ( "An error has occurred, not all lists are equal." )
		
			
if __name__ == "__main__":
	main()
	
	
	
