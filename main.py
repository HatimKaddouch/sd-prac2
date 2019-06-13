import sys
import pywren-ibm-cloud as pywren

def main():
    if len(sys.argv) != 2:
        print("ERROR: Insuficientes agrumentos")
        exit(1)
    try:
        N = int(sys.argv[1])
    except:
        print("ERROR: El parámetro ha de ser un entero")
        exit(2)
    pw = pywren.ibm_cf_executor()

    pw.call_async(my_function_leader, N)



    pw = pywren.ibm_cf_executor()

    pw.map(my_function_slave, range(total))


    results = pw.get_result()
    exit(0)

def my_function_leader(N):
    messages = 0
    maps = N
    petitions = []
    def callback(ch, method, properties, body):
        nonlocal petitions
        nonlocal messages
        nonlocal maps
        data = body.decode()
        petitions.append(data)
        messages += 1
        if messages == maps:
            ch.stop_consuming()
            messages = 0

    pw_config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    params = pika.URLParameters(pw_config['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel

    channel.queue_declare('leader')

    for i in range(maps):
        petitions = []

        channel.basic_consume(callback,
                              queue='leader',
                              no_ack=True)
        channel.start_consuming()
        
        slave = random.choice(leader)
        channel.basic_publish(exchange='',
                          routing_key='slave'+str(slave),
                          body=json.dumps(result))

def my_function_slave(args):


if __main__ == "__main__":
    main()
