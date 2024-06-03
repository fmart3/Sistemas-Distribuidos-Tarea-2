import Kafka from 'node-rdkafka';

const consumer = Kafka.KafkaConsumer({
    'group.id': 'procesamiento',
    'metadata.broker.list': 'localhost:9092'
}, {});

const Estados = {
    'recibido': 10000,
    'preparando': 10000,
    'entregando': 10000
}

consumer.connect();

consumer.on('ready', () => {
    console.log('Procesamiento listo...');
    consumer.subscribe(['solicitud-producto']);
    consumer.consume();
}).on('data', (data) => {
    const message = JSON.parse(data.value.toString());

    
        
        message.estado = 'recibido';
        console.log('Estado inicial asignado:', message);
        enviarAMensajeProcesado(message);
        
    
    
        switch (message.estado) {
            case 'recibido':
                message.estado = 'preparando';
                break;
            case 'preparando':
                message.estado = 'entregando';
                break;
            case 'entregando':
                message.estado = 'finalizado';
                break;
        }
    console.log('Estado actualizado:', message);
    
    enviarAMensajeProcesado(message);

    
    
});

function enviarAMensajeProcesado(message) {
    const producer = Kafka.Producer.createWriteStream({
        'metadata.broker.list': 'localhost:9092'
    }, {}, { topic: 'procesamiento-producto' });

    const success = producer.write(Buffer.from(JSON.stringify(message)));
    if (success) {
        console.log('Mensaje procesado enviado:', message);
    } else {
        console.log('Error al enviar mensaje procesado');
    }
}

