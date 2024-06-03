import Kafka from 'node-rdkafka';
import express from 'express';
const app = express();
const notifications = new Map();

const consumer = Kafka.KafkaConsumer({
    'group.id': 'notificaciones',
    'metadata.broker.list': 'localhost:9092'
}, {});

consumer.connect();

consumer.on('ready', () => {
    console.log('Consumer listo...');
    consumer.subscribe(['procesamiento-producto']);
    consumer.consume();
}).on('data', (data) => {
    const message = JSON.parse(data.value.toString());
    console.log('Notificación recibida:', message);
    notifications.set(message.id,message);

    
});

app.get('/solicitud/:id', (req, res) => {
    const id = parseInt(req.params.id);
    if (notifications.has(id)) {
        res.status(200).json(notifications.get(id));
    } else {
        res.status(404).send({ error: 'Solicitud no encontrada' });
    }
});

app.listen(3001, () => {
    console.log('Servidor de notificaciones escuchando en el puerto 3001');
});

