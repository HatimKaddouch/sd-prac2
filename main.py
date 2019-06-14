import sys
import pywren_ibm_cloud as pywren
import pika

def main():
    args = []

    if len(sys.argv) != 2:
        print("ERROR: Insuficientes agrumentos")
        exit(1)
    try:
        N = int(sys.argv[1])
    except:
        print("ERROR: El parámetro ha de ser un entero")
        exit(2)

    pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)

    params = pika.URLParameters(pw.config['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel

    channel.exchange_declare(exchange='publish_subscribe')

    pw.call_async(my_function_leader, N)

    pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)

    d = {'N':N}
    for i in range(N):
        d['id'] = i
        args.append(d.copy())

    pw.map(my_function_slave, args)

    results = pw.get_result()

    print(results)
    exit(0)

def my_function_leader(N): #N = num maps
    messages = 0
    maps = N
    petitions = []
    i = 0

    def callback(ch, method, properties, body):
        nonlocal petitions
        nonlocal messages
        nonlocal maps
        nonlocal i
        data = body.decode('utf-8')
        petitions.append(data)
        messages += 1
        if messages == maps - i:
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

        slave = random.choice(petitions)
        channel.basic_publish(exchange='',
                          routing_key='slave'+str(slave),
                          body='write_allowed')

    channel.queue_delete(queue='leader')
    connection.close()

def my_function_slave(id, N): #N = num maps
    randoms = []
    messages = 0
    maps = N
    i = id
    write = False
    def callback(ch, method, properties, body):
        nonlocal randoms
        nonlocal messages
        nonlocal maps
        nonlocal i
        nonlocal write
        data = body.decode('utf-8')
        if data == 'write_allowed':
            rand = random.randint(0,500)
            channel.basic_publish(exchange='publish_subscribe',
                              routing_key='',
                              body=str(rand))
            write = True
        else:
            randoms.append(int(data))
            messages += 1
            if messages == maps:
                ch.stop_consuming()
            elif not write:
                channel.basic_publish(exchange='',
                                  routing_key='leader',
                                  body=str(id))

    pw_config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    params = pika.URLParameters(pw_config['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel

    channel.queue_declare('slave'+str(id))
    channel.queue_bind(exchange='publish_subscribe',queue='slave'+str(id))

    channel.basic_publish(exchange='',
                      routing_key='leader',
                      body=str(id))

    channel.basic_consume(callback,
                          queue='slave'+str(id),
                          no_ack=True)
    channel.start_consuming()
    channel.queue_delete(queue='slave'+str(id))
    connection.close()

    return(randoms)

if __name__ == "__main__":
    main()
