import Kafka from 'node-rdkafka';
import express from 'express';

const app = express();
app.use(express.json()); 

const producer = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, { topic: 'solicitud-producto' });

app.post('/solicitud', (req, res) => {
    const message = req.body;

    message.id = Date.now();

    const success = producer.write(Buffer.from(JSON.stringify(message)));
    if (success) {
        console.log('Mensaje enviado:', message);
        res.status(200).send({ id: message.id, status: 'Solicitud recibida' });
    } else {
        console.log('ERROR');
        res.status(500).send({ error: 'Error al procesar' });
    }
});

app.listen(3000, () => {
    console.log('Servidor escuchando en el puerto 3000');
});
