require('dotenv').config();
const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER] });
const producer = kafka.producer();

app.use(express.json());

app.post('/sensor-data', async (req, res) => {
  const data = req.body;
  try {
    await producer.connect();
    await producer.send({
      topic: process.env.KAFKA_TOPIC,
      messages: [{ value: JSON.stringify(data) }],
    });
    console.log('Data sent to Kafka:', data);
    res.status(200).send('Data received');
  } catch (err) {
    console.error('Error:', err);
    res.status(500).send('Error sending data');
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Producer server running on port ${PORT}`);
});
